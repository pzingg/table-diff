#!/usr/bin/ruby
require 'dbi'

# === Algorithm
#
# The aim of the algorithm is to compare the content of two tables,
# possibly on different remote servers, with minimum network traffic.
# It is performed in three phases. 
#
# * A checksum table is computed on each side for the target table.
# * A first level summary table is computed on each side by aggregating chunks
#   of the checksum table. Other levels of summary aggregations are then performed
#   till there is only one row in the last table, which then stores a 
#   global checksum for the whole initial target tables. 
# * Starting from the upper summary tables, aggregated checksums are compared 
#   from both sides to look for differences, down to the initial checksum table.
#   Keys of differing tuples are displayed.
#
# ==== Checksum Table
#
# The first phase computes the initial checksum table <em>t(0)</em> on each side.
# Assuming that <em>key</em> is the table key columns, and <em>cols</em> is the
# table data columns that are to be checked for differences, then
# it is performed by querying target table <em>t</em> as follows:
#
#   CREATE TABLE t(0) AS
#   SELECT key AS id, checksum(key) AS idc, checksum(key || cols) AS cks
#   FROM t;
#
# The initial key is kept, as it will be used to show differing keys
# at the end. The rational for the <em>idc</em> column is to randomize the 
# key-values distribution so as to balance aggrates in the next phase.
# The key must appear in the checksum also, otherwise content exchanged 
# between two keys would not be detected in some cases.
#
# ==== Summary Tables
#
# Now we compute a set of cascading summary tables by grouping <em>f</em>
# (folding factor) checksums together at each stage. The grouping is
# based on a mask on the <em>idc</em> column to take advantage of the 
# checksum randomization. Starting from <em>p=0</em> we build:
#
#   CREATE TABLE t(p+1) AS
#   SELECT idc & mask(p+1) AS idc, XOR(cks)
#   FROM t(p)
#   GROUP BY idc & mask(p+1);
# 
# The <em>mask(p)</em> is defined so that it groups together on average <em>f</em>
# checksums together: <em>mask(0) = ceil2(size); mask(p) = mask(p-1)/f</em>.
# This leads to a hierarchy of tables, each one being a smaller summary
# of the previous one:
#
# * level 0: checksum table, <em>size</em> rows, i.e. as many rows as the target table.
# * level 1: first summary table, <em>(size/f)</em> rows.
# * level <em>p</em>: intermediate summary table, <em>(size/f**p)</em> rows.
# * level <em>n-1</em>: one before last summary table, less than <em>f</em> rows.
# * level <em>n</em>: last summary table, mask is 0, 1 row.
#
# It is important that the very same masks are used so that aggregations
# are the same, allowing to compare matching contents on both sides.
#
# ==== Search for Differences
#
# After all these support tables are built on both sides comes the search for
# differences. When checking the checksum summary of the last tables (level <em>n</em>)
# with only one row, it is basically a comparison of the checksum of the 
# whole table contents. If they match, then both tables are equal, 
# and we are done. Otherwise, if these checksums differ, some investigation 
# is needed to detect offending keys.
# 
# The investigation is performed by going down the table hierarchy and
# looking for all <em>idc</em> for which there was a difference in the checksum
# on the previous level. The same query is performed on both side
# at each stage:
# 
#   SELECT idc, cks
#   FROM t(p)
#   WHERE idc & mask(p+1) IN (idc-with-diff-checksums-from-level-p+1)
#   ORDER BY idc, cks;
#
# And the results from both sides are merged together. 
# When doing the merge procedure, four cases can arise:
# 
# 1. Both <em>idc</em> and <em>cks</em> match. Then there is no difference.
# 2. Although <em>idc</em> does match, <em>cks</em> does not. Then this <em>idc</em> is
#    to be investigated at the next level, as the checksum summary differs.
#    If we are already at the last level, then the offending key can be shown.
# 3. No <em>idc</em> match, one supplemental <em>idc</em> in the first side.
#    Then this <em>idc</em> correspond to key(s) that must be inserted
#    for syncing the second table wrt the first.
# 4. No <em>idc</em> match, one supplemental <em>idc</em> in the second side.
#    Then this <em>idc</em> correspond to key(s) that must be deleted
#    for syncing the second table wrt the first.
#
# Cases 3 and 4 are simply symmetrical, and it is only an interpretation 
# to decide whether it is an insert or a delete, taking the first side 
# as the reference.
# 
# === Implementation Issues
#
# * The checksum implementation gives integers, which are constant length
#   and easy to manipulate afterwards.
# * The xor aggregate is a good choice because there is no overflow issue with it
#   and it takes into account all bits of the input.
# * Null values must be taken care appropriately.
# * The folding factor and all modules are taken as a power of two.
# 
# There is a special management of large chunks of deletes or inserts
# which is implemented although not detailed in the algorithmic overview
# above nor the complexity analysis below.
#
# === Analysis
#
# Let <em>n</em> be the number of rows, <em>r</em> the row size, <em>f</em> the folding factor
# and <em>k</em> the number of differences to be detected. Then ISTM that:
#
# * <b>Network volume</b> is better than <em>k*f*ceil(log(n)/log(f))</em>: it is independent of <em>r</em>, 
#   the lower <em>f</em> the better, and you want <em>k<<n</em>.
# * <b>Number of requests<b>: The maximum is <em>6+2*ceil(log(n)/log(f))</em>, and the
#   minimum is <em>6+ceil(log(n)/log(f))</em> for equal tables.
# * <b>Disk I/O traffic</b> is about <em>n*r+n*ln(n)*(f/(f-1))</em>. Here a not too small <em>f</em> is better, as 
#   it reduces both the number of requests and of disk I/Os.
#
# The choice of <em>f</em> is indeed a tradeoff.
#
# === References
#
# This script and algorithm was somehow inspired by:
# 
# <em>Taming the Distributed Database Problem: A Case Study Using MySQL</em>
# by Giuseppe Maxia in <b>Sys Admin</b> vol 13 num 8, Aug 2004, pp 29-40.
# See http://www.perlmonks.org/index.pl?node_id=381053 for details.
#
# In the above paper, three algorithms are presented. 
# The first one compares two tables with a checksum technique.
# The second one finds UPDATE or INSERT differences based on a 2-level 
# (checksum and summary) table hierarchy. The algorithm is asymmetrical,
# as different queries are performed on the two tables to be compared.
# It seems that the network traffic volume is in <em>k*(f+(n/f)+r)</em>,
# that it has a probabilistically-buggy merge procedure, and
# that it makes assumptions about the distribution of key values.
# The third algorithm looks for DELETE differences based on counting,
# with the implicit assumption that there are only such differences.
#
# The algorithm used here implements all three tasks. It is fully symmetrical.
# It finds UPDATE, DELETE and INSERT between the two tables. 
# The checksum and summary hierarchical level idea is reused and generalized
# so as to reduce the algorithmic complexity.
#
# From the implementation standpoint, the script is as parametric 
# as possible thru many options, and makes as few assumptions 
# as possible about table structures, types and values.
#
# === See Also
#
# Some products implement such features:
#
# * http://www.programurl.com/software/sql-server-comparison.htm
# * http://www.dbbalance.com/db_comparison.htm
# * http://www.dkgas.com/dbdiff.htm
# * http://www.sql-server-tool.com/
# * http://sourceforge.net/projects/mysqltoolkit

# ==== TableDiff - Compare the data in two SQL tables using DBI library
#
# Author::    Peter Zingg mailto:pzingg@kentfieldschools.org
# Copyright:: Copyright (C) 2007  Peter F. Zingg, Kentfield School District
# License::   Distributes under the same terms as Ruby
#
# ==== Example
#
#   dbh = DBI.connect('dbi:mysql:diffs', 'username', 'password')
#   db_opts = { :dbh => dbh, :cols => 'first_name,last_name,schoolid' }
#   t1_opts = db.opts.merge(:table => 'master') # the 'master' or current database table
#   t2_opts = db.opts.merge(:table => 'local')  # the 'local' database table that needs to be synced
#   td = TableDiff.new(t1_opts, t2_opts, :threads => true, :stats => true)
#   td.process do |change_type, id|
#     print "type of change: #{change_type}, record key: #{id}\n"
#   end
#
# ==== Description
#
# This class encapsulates the DBI connections to two tables whose
# contents are to be compared.  For each connection, you must specify
# a table name, the columns within the table that will be compared,
# and the columns that will be used as a primary key to identify
# the same records within each table.
#
# The class performs a network and time efficient comparison of two
# possibly large tables on two servers. It makes only sense to use it
# if the expected differences are small.
# 
# The implementation is quite generic: multi-column keys, no assumption
# of data types other that they can be cast to text, subset of columns
# can be used for the comparison, handling of null values, etc.
#
# In case of key-checksum or data-checksum collision, false positive or
# false negative results may occur. Changing the checksum function would
# help in such cases.
#
# ==== Dependencies
#
# The class uses the Ruby/DBI module and requires a corresponding DBD driver for the 
# databases that are accessed.  The code has been tested with MySQL 5 only! 
# See http://ruby-dbi.rubyforge.org for more information.
#
# Three support functions are needed on the database:
#
# * The <tt>WS_CONCAT</tt> function defined by MySQL takes care of NULL values in columns.
#   For Postgresql the <tt>COALESCE</tt> function can be used.  It may be changed with the
#   <tt>:concat</tt> option.
# * A checksum function must be used to reduce and distribute key and columns values. By
#   default, the <tt>CRC32</tt> function defined by MySQL is used.  It may be changed with 
#   the <tt>:checksum</tt> option.  The PostgreSQL <tt>MD5</tt> function can be used for this purpose.
#   Three other suitable implementations are available for PostgreSQL 
#   and can be loaded into the server by processing <tt>share/contrib/checksum.sql</tt>.
#   The three functions <tt>cksum2</tt>, <tt>cksum4</tt> and <tt>cksum8</tt> differ on the size
#   of the resulting checksum expressed in bytes. The original pg_comparator program used
#   the <tt>cksum8</tt> version.
# * An aggregate function is used to summarize checksums for a range of rows.  It must operate 
#   on the result of the checksum function.  By default the <tt>BIT_XOR</tt> function defined
#   by MySQL is used.  It may be changed with the <tt>:aggregate</tt> option.
#   Suitable implementations of an exclusive-or <tt>xor</tt> aggregate are available 
#   for PostgreSQL and can be loaded into the server by processing <tt>share/contrib/xor_aggregate.sql</tt>.
#
# ==== Notes
#
# This class is a Ruby/DBI port of the pg_comparator perl program
# written for Postgresql by Fabian Coelho.  See http://pgfoundry.org/projects/pg-comparator for the latest
# version of the perl source on which this port was based.
#
# Fabian Coelho's site for the pg_comparator tool is http://www.coelho.net/pg_comparator
#
# Coelho's work was also inspired by Giuseppe Maxia.  See http://www.perlmonks.org/index.pl?node_id=381053 for details.
#
# Another MySQL implementation can be found at: http://www.xaprb.com/blog/2007/03/18/introducing-mysql-table-sync/
# 
# ==== Original Copyright and License for pg_comparator perl source
# 
# Copyright (c) 2004-2007, Fabien Coelho <fabien at coelho dot net>,
# http://www.coelho.net
#
# pg_comparator is distributed under the terms of the BSD Licence.
# See http://pgfoundry.org/projects/pg-comparator for more information.

class TableDiff
  
  # Sets the parameters for beginning a difference comparison of two
  # database tables.
  #
  # <tt>table1</tt>::  A symbol-keyed hash specifying the connection parameters for
  #                    the local, or current state of the data ("table 1").
  # <tt>table2</tt>::  A symbol-keyed hash specifying the connection parameters for
  #                    the remote, or previous state of the data ("table 2").
  # <tt>options</tt>:: (Optional) A symbol-keyed hash specifying options
  #                    used to control processing.
  #
  # The parameters for the table1 and table2 hashes are:
  #
  # <tt>:dbh</tt>::    An open DBI connection to the database containing the table.
  # <tt>:table</tt>::  The (optionally schema-qualified) name of the table in the database.
  # <tt>:cols</tt>::   A comma-delimited string of the data column names within the table to be compared.
  # <tt>:keys</tt>::   A comma-delimited string of the index column names within the table that constitute a primary key.
  # <tt>:keylen</tt>:: An integer specifying the string length to accomodate the primary key (default: 255).
  #
  # Parameters not specified for table 2 will be copied from those for table 1.
  #
  # The processing options available are:
  # 
  # <tt>:max_levels</tt>::  Maximum number of levels used. Allows to cut-off folding.
  #                         Default is 0, meaning no cut-off. Setting a value of 1
  #                         only uses the checksum table, without summaries.
  # <tt>:max_report</tt>::  Maximum search effort, search will stop if above this threshold: 
  #                         It gives up if a single query at any level involves more than this 
  #                         many differences. Use 0 for no limit.  Default value is 32.
  # <tt>:factor</tt>::      Folding factor: log2 of the number of rows grouped together at each stage.
  #                         Default value of 7 was chosen after some basic tests on medium-size cases.
  # <tt>:sep</tt>::         Separator string when concatenating columns (default: <tt>':'</tt>).
  # <tt>:num_records</tt>:: Assume the provided value as the table size, thus skipping the COUNT query.
  # <tt>:where</tt>::       SQL boolean condition for partial comparison (default: nil).
  # <tt>:prefix</tt>::      Name prefix for comparison tables (default: <tt>'cmp'</tt>). May be schema-qualified.
  # <tt>:null</tt>::        String processing template to take care of null values. 
  #                         If you set <tt>:null => '%s'</tt>, null values are set as they appear,
  #                         which might damage the results in null columns are used because 
  #                         multi-column checksums will all be 0. Default is <tt>'COALESCE(%s,\'null\')'</tt>.
  # <tt>:concat</tt>::      String processing template to concatenate field values.
  #                         Default is <tt>'CONCAT_WS(\':\',%s)'</tt>.
  # <tt>:checksum</tt>::    A SQL checksum function to be used (default: <tt>'CRC32'</tt>).
  #                         The quality of this function in term of bit-spreading and 
  #                         uniformity is important for the quality of the
  #                         results. A poor function might miss differences because of collisions
  #                         or result in a more costly search. Cryptographic hash functions such as 
  #                         MD5 or SHA1 are a good choice.
  # <tt>:aggregate</tt>::   Name of the SQL aggregation function to be used for summaries.  
  #                         Must operate on the result of the checksum function.  Default is <tt>'XOR'</tt>.
  # <tt>:temporary</tt>::   Whether to use temporary tables (default: true). If you don't, the tables are kept at the end, so they will have to be deleted by hand.
  # <tt>:cleanup</tt>::     Drop checksum tables (default: true only <tt>:temporary</tt> is false).  Set to false if you want to see the processing data.
  # <tt>:threads</tt>::     If true, use threads: a synonymous for "segmentation fault" :-) (default: false).
  # <tt>:stats</tt>::       If true, show various statistics (default: false).
  # <tt>:verbose</tt>::     Verbosity level (default: 0) from 0 to 3.  Processing progress will be written to $stderr if this is greater than 0.
  
  def initialize(table1, table2, options={})
    @table1            = table1.dup
    @table2            = table2.dup
    @table1[:keys]   ||= 'id'
    @table1[:keylen] ||= 255
    @table2[:dbh]    ||= @table1[:dbh]
    @table2[:table]  ||= @table1[:table]
    @table2[:cols]   ||= @table1[:cols]
    @table2[:keys]   ||= @table1[:keys]
    @table2[:keylen] ||= @table1[:keylen]
    @dbh1        = @table1[:dbh]
    @dbh2        = @table2[:dbh]
    @verbose     = options[:verbose]     || 0
    @max_levels  = options[:max_levels]  || 0
    @max_report  = options[:max_report]  || 32
    @factor      = options[:factor]      || 7
    @sep         = options[:sep]         || ':'
    @num_records = options[:num_records] || 0
    @where       = options[:where]
    @prefix      = options[:prefix]      || 'cmp'
    @nul         = options[:null]        || 'COALESCE(%s,\'null\')'
    @concat      = options[:concat]      || 'CONCAT_WS(\':\',%s)'
    @checksum    = options[:checksum]    || 'CRC32'     # pg_comparator had 'CKSUM8'
    @aggregate   = options[:aggregate]   || 'BIT_XOR'   # pg_comparator had extension function 'xor'
    @temporary   = options.key?(:temporary) ? options[:temporary] : true
    @cleanup     = options.key?(:cleanup)   ? options[:cleanup]   : !@temporary
    @threads     = options.key?(:threads)   ? options[:threads]   : false
    @stats       = options.key?(:stats)     ? options[:stats]     : false
  end

  # Do the actual processing of the comparison.  If a block is given, the block
  # will be called on each change detected, and will be passed two arguments:
  #
  # * <tt>change_type</tt>: A symbol, either <tt>:insert</tt>, <tt>:delete</tt>, or <tt>:update<tt>, describing the type of change to the record
  # * <tt>record_key</tt>: The index for this record, with individual elements of the index delimited by the <tt>:sep</tt> option.
  #
  # If no block is given, changes will be printed on $stdout. The output consists of lines 
  # describing the differences found between the two tables. They are expressed in term of insertions,
  # updates or deletes and of tuple key, formatted like this:
  #
  # * update <record_key>: The tuple is updated from table 1 to table 2.  It exists in both tables with different values.
  # * insert <record_key>: The tuple does not appear in table 2, but only in table 1. It must be inserted in table 2 to synchronize it wrt table 1.
  # * delete <record_key>: The tuple appears in table 2, but not in table 1. It must be deleted from 2 to synchronize it wrt table 1.
  
  def process(&block)
    # sanity checking
    raise "no dbh1" unless @dbh1
    raise "no table1" unless @table1[:table]
    raise "no keys1" unless @table1[:keys]
    raise "no cols1" unless @table1[:cols]
    raise "no dbh2" unless @dbh2
    raise "no table2" unless @table2[:table]
    raise "no keys2" unless @table2[:keys]
    raise "no cols2" unless @table2[:cols]  
    
    # default output if not specified
    block = lambda { |change_type, id| puts "#{change_type} #{id}" } if block.nil?
    t0 = Time.now if @stats
    
    # fix factor size
    @factor = 1 if @factor < 1
    @factor = 30 if @factor > 30

    # intermediate table names
    name1 = "#{@prefix}_1_"
    name2 = "#{@prefix}_2_"
    
    count1 = 0
    count2 = 0    
    if (@threads)
      thread1 = Thread.new { count1 = compute_checksum(@dbh1, @table1[:table], @table1[:keys], @table1[:keylen], @table1[:cols], name1) }
      thread2 = Thread.new { count2 = compute_checksum(@dbh2, @table2[:table], @table2[:keys], @table1[:keylen], @table2[:cols], name2) }
      thread1.join
      thread2.join
    else
      count1 = compute_checksum(@dbh1, @table1[:table], @table1[:keys], @table1[:keylen], @table1[:cols], name1)
      count2 = compute_checksum(@dbh2, @table2[:table], @table2[:keys], @table1[:keylen], @table2[:cols], name2)
    end
    size = count1 > count2 ? count1 : count2
    raise "nothing to do" if size == 0

    $stderr.print("# computing masks based on folding factor...\n") if @verbose > 0
    $stderr.print("# size is #{size}, factor is #{@factor}\n") if @verbose > 2
    masks = []
    0.upto(size) do |i|
      mask = (1 << (i*@factor)) - 1
      masks.unshift(mask)
      break if mask >= size
    end
    
    calc_levels = masks.size
    $stderr.print("# number of masks is #{calc_levels}, max_levels is #{@max_levels}\n") if @verbose > 2
    masks[@max_levels..levels-1] = nil if @max_levels > calc_levels # cut-off
    levels = masks.size
    $stderr.print("# masks = #{masks.inspect}\n") if @verbose > 2
    tcks = Time.now if @stats

    $stderr.print("# building summary tables...\n") if @verbose > 0
    if (@threads)
      thread1 = Thread.new { compute_summaries(@dbh1, name1, masks) }
      thread2 = Thread.new { compute_summaries(@dbh2, name2, masks) }
      thread1.join
      thread2.join
    else
      compute_summaries(@dbh1, name1, masks)
      compute_summaries(@dbh2, name2, masks)
    end
    tsum = Time.now if @stats
    
    $stderr.print("# looking for differences...\n") if @verbose > 0
    diffs = differences(@dbh1, @dbh2, name1, name2, masks, block)
    $stderr.print("# diffs = #{diffs.inspect}\n") if @verbose > 2
    tmer = Time.now if @stats

    # now take care of big chunks of INSERT or DELETE if necessary
    # should never happen in normal "few differences" conditions
    bdel = diffs[:mask_delete]
    bins = diffs[:mask_insert]
    $stderr.print("bulk delete: #{bdel.size}\n") if !bdel.empty? && @verbose
    $stderr.print("bulk insert: #{bins.size}\n") if !bins.empty? && @verbose

    insb = []
    delb = []
    if (!bins.empty? || !bdel.empty?)
      # this cost two full table-0 scans, one on each side...
      if @threads
        thread1 = Thread.new { insb = get_bulk_keys(@dbh1, "#{name1}0", :insert, bins, block) }
        thread2 = Thread.new { delb = get_bulk_keys(@dbh2, "#{name2}0", :delete, bdel, block) }
        thread1.join
        thread2.join
      else
        insb = get_bulk_keys(@dbh1, "#{name1}0", :insert, bins, block)
        delb = get_bulk_keys(@dbh2, "#{name2}0", :delete, bdel, block)
      end
    end
    
    if @cleanup
      cleanup_tables(@dbh1, name1, levels)
      cleanup_tables(@dbh2, name2, levels)
    end
    tblk = Time.now if @stats

    # update count with bulk contents
    count = diffs[:count] + insb.size + delb.size
    $stderr.print("# done, #{count} differences found...\n") if @verbose > 0
    
    if @stats
      # summary of performances
      print "table diff statistics: " + (@threads ? 'using threads' : 'not using threads') +
        "\n   table count: #{size}\n" +
        "folding factor: #{@factor}\n" +
        "        levels: #{levels} (reduced from #{calc_levels})\n" +
        "   diffs found: #{count}\n" +
        "    total time: #{tblk-t0}\n" +
        "      checksum: #{tcks-t0}\n" +
        "       summary: #{tsum-tcks}\n" +
        "         merge: #{tmer-tsum}\n" +
        "         bulks: #{tblk-tmer}\n"
    end
  end
  
  protected

  def subs(fmt, fields) # :nodoc:
    fields.split(/,/).collect { |s| fmt.gsub(/\%s/, s) }
  end

  def concat_simple(fields) # :nodoc:
    @concat.gsub(/\%s/, fields)
  end
  
  # returns a sql concatenation of coalesced fields 
  def concat_null(fields) # :nodoc:
    @concat.gsub(/\%s/, subs(@nul, fields).join(','))
  end
  
  def temp # :nodoc:
    @temporary ? 'TEMPORARY ' : ''
  end
    
  def compute_checksum(dbh, table, keys, keylen, cols, name) # :nodoc:
    $stderr.print("building checksum table #{name}0\n") if @verbose > 1
    dbh.execute("DROP TABLE IF EXISTS #{name}0") if @cleanup
    
    all_cols = "#{keys},#{cols}"
    query = "CREATE #{temp}TABLE #{name}0 (" +
      "id VARCHAR(#{keylen}) NOT NULL, " +
      "idc INTEGER UNSIGNED NOT NULL, " + 
      "cks INTEGER UNSIGNED NOT NULL" +
      ") AS SELECT #{concat_null(keys)} AS id, " +
      "#{@checksum}(#{concat_null(keys)}) AS idc, " +
      "#{@checksum}(#{concat_null(all_cols)}) AS cks " +
      "FROM #{table}"
    query += " WHERE #{@where}" if @where
    $stderr.print("#{query}\n") if @verbose > 2
    dbh.execute(query)
    
    # count should be available somewhere?
    count = @num_records
    if count == 0
      row = dbh.select_one("SELECT COUNT(*) FROM #{name}0")
      count = row[0] if row
    end
    return count
  end
  
  def compute_summaries(dbh, name, masks) # :nodoc:
    # compute cascade of summary tables
    masks.each_with_index do |mask, level|
      next if level == 0
      
      $stderr.print("building summary table #{name}#{level} (#{mask})\n") if @verbose > 1
      dbh.execute("DROP TABLE IF EXISTS #{name}#{level}") if @cleanup
      # the "& mask" is really a modulo operation
      query = "CREATE #{temp}TABLE #{name}#{level} (" +
        "idc INTEGER UNSIGNED NOT NULL, " + 
        "cks INTEGER UNSIGNED NOT NULL" +
        ") AS SELECT idc & #{mask} AS idc, #{@aggregate}(cks) AS cks " +
        "FROM #{name}#{level-1} GROUP BY idc & #{mask}"
      $stderr.print("#{query}\n") if @verbose > 2
      dbh.execute(query)
    end
  end
  
  # get info for investigated a list of idc (hopefully not too long)
  # sth = selidc(dbh, table, mask, get_id, idc)
  def selidc(dbh, table, mask, get_id, idc) # :nodoc:
    query = "SELECT idc, cks" + (get_id ? ', id ' : ' ') + "FROM #{table} "
    # the "& mask" is really a modulo operation
    query += "WHERE idc & #{mask} IN (" + idc.join(',') + ') ' if !idc.empty?
    query += 'ORDER BY idc, cks';
    $stderr.print("#{query}\n") if @verbose > 2
    dbh.execute(query)
  end
  
  # compute differences by climbing up the tree, output result on the fly.
  def differences(dbh1, dbh2, name1, name2, masks, block) # :nodoc:
    idc   = []
    mask_insert = []
    mask_delete = []
    mask  = 0 # mask of previous table
    count = 0
    inserts = 0
    updates = 0
    deletes = 0
    (masks.size-1).downto(0) do |level|
      next_idc = []
      $stderr.print("investigating #{idc.inspect}, level=#{level}\n") if @verbose > 1
      raise "giving up at level #{level}: #{idc.size} differences exceeeds max_report (#{@max_report})" if (@max_report > 0 && idc.size > @max_report)

      # select statement handlers
      s1 = selidc(dbh1, "#{name1}#{level}", mask, level == 0, idc)
      s2 = selidc(dbh1, "#{name2}#{level}", mask, level == 0, idc)
      r1 = nil
      r2 = nil
      s1_active = true
      s2_active = true
      
      # content of one row from the above select result
      # let us merge the two ordered select
      while (true) do
        # update current lists if necessary
        if !r1 && s1_active
          r1 = s1.fetch
          s1_active = false unless r1
        end
        if !r2 && s2_active
          r2 = s2.fetch
          s2_active = false unless r2
        end
        break unless (r1 || r2)
        
        # else both lists are defined
        if (r1 && r2 && r1[0] == r2[0]) # matching idc
          if (r1[1] != r2[1]) # non matching checksums
            if (level != 0)
              next_idc.push(r1[0]) # to be investigated...
            else
              # the level-0 table keeps the actual key
              count += 1
              updates += 1
              block.call(:update, r1[2]) unless block.nil? # final result
            end
          end
          # both tuples are consummed
          r1 = nil
          r2 = nil
          
        # if they do not match, one is missing or less than the other
        elsif (!r2 || (r1 && r1[0] < r2[0])) # more idc in table 1
          if (level != 0)
            mask_insert.push([ r1[0], masks[masks.size-1] ]) # later
          else
            count += 1
            inserts += 1
            block.call(:insert, r1[2]) unless block.nil?  # final result
          end
          # left tuple is consummed
          r1 = nil
          
        # this could be a else
        elsif (!r1 || (r2 && r1[0] > r2[0])) # more idc in table 2
          if (level != 0)
            mask_delete.push([ r2[0], masks[masks.size-1] ]) # later
          else
            count += 1
            deletes += 1
            block.call(:delete, r1[2]) unless block.nil?  # final result
          end
          # right tuple is consummed
          r2 = nil
        else
          raise 'this state should never happen'
        end
      end
      s1.finish
      s2.finish
      level -= 1 # next table! 0 is the initial checksum table
      mask = masks.pop() # next mask
      idc  = next_idc # idcs to be investigated on next round
      break if idc.empty?
    end
    {
      :count => count,
      :inserts => inserts,
      :updates => updates,
      :deletes => deletes,
      :mask_insert => mask_insert,
      :mask_delete => mask_delete
    }
  end
  
  # investigate an "idc/mask" list to show corresponding keys.
  def get_bulk_keys(dbh, table, change_type, idc_masks, block) # :nodoc:
    return [] if idc_masks.empty? # bye if nothing to investigate
    cond = '' # select query condition. must not be empty.
    $stderr.print("# investigating #{change_type} chunks\n") if @verbose > 0
    idc_masks.each do |idc, mask|
      cond << ' OR ' if cond
      cond << "idc & #{mask}=#{idc}"
    end
    keys = [] # results
    query = "SELECT id FROM #{table} WHERE #{cond} ORDER BY id"
    $stderr.print("#{query}\n") if @verbose > 2
    sth = dbh.execute(query)
    sth.each do |row|
      keys.push(row[0])
      block.call(change_type, row[0]) unless block.nil?
    end
    keys
  end
  
  def cleanup_tables(dbh, name, levels) # :nodoc:
    (levels-1).downto(0) do |level|
      $stderr.print("dropping table #{name}#{level}\n") if @verbose > 2
      dbh.execute("DROP TABLE IF EXISTS #{name}#{level}")
    end
  end
end


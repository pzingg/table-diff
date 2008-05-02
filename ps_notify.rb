#!/usr/bin/ruby 

# prepend the current directory to the ruby module search list
$:.unshift(File.expand_path(File.dirname(__FILE__))) unless
  $:.include?(File.dirname(__FILE__)) || $:.include?(File.expand_path(File.dirname(__FILE__)))

require 'rubygems'
require 'dbi'
require 'net/smtp'
require 'action_mailer' # or install and require 'tmail'

# table_diff contains the TableDiff class that does the work
require 'table_diff'

# app_config contains the APP_CONFIG global used for smtp and database 
# connections.  copy app_config.rb.sample and edit to suit your tastes.
require 'app_config'

# powerschool data schema
require 'ps_columns'
include PowerSchool

# pulls the time that the table was last modified
# only works with mysql's MyISAM tables (uses the file mod time)
def table_update_time(dbh, table)
  begin
    sth = dbh.execute("SHOW TABLE STATUS WHERE Name='#{table}'")
    row = sth.fetch_hash
  rescue
    row = nil
  end
  row.nil? ? nil : row['Update_time']
end

# if records exist in both local and remote versions
# (change type is "update"), find out which columns have different
# values and return the changes in a list of tuples
def get_changes(dbh, table, cols, keys, id)
  id_parts = id.split(/:/)
  where = ''
  keys.split(/,/).each_with_index do |key, i|
    where << " AND " if where != ''
    where << "#{key}=#{dbh.quote(id_parts[i])}"
  end
  row      = dbh.select_one("SELECT #{cols} FROM #{table} WHERE #{where}")
  row_last = dbh.select_one("SELECT #{cols} FROM #{table}_last WHERE #{where}")
  changes = []
  cols.split(/,/).each_with_index do |col, i|
    changes.push([col, row[i], row_last[i]]) if "#{row[i]}" != "#{row_last[i]}"
  end
  changes
end

# initialize the TableDiff class and process the differences
# if update is found, dig into data to find which columns were changed
# then yield change information to the calling block
def diff_table(dbh, table, cols, keys, options={})
  t1_update_time = table_update_time(dbh, table)
  t2_update_time = table_update_time(dbh, "#{table}_last")
  t1_opts = { :dbh => dbh, :table => table, :cols => cols, :keys => keys }
  t2_opts = { :dbh => dbh, :table => "#{table}_last", :cols => cols, :keys => keys }
  td = TableDiff.new(t1_opts, t2_opts, options.merge(:cleanup => true))
  td.process do |change_type, id|
    changes = (change_type == :update) ? get_changes(dbh, table, cols, keys, id) : nil
    yield change_type, id, t1_update_time, t2_update_time, changes
  end
end


# rename a SQL table
def rotate_table(dbh, table)
  begin
    dbh.execute("DROP TABLE IF EXISTS #{table}_last")
    dbh.execute("RENAME TABLE #{table} TO #{table}_last") 
  rescue 
  end
end

# copy table data from dbh_src to dbh_dst for processing
# rename existing table data in dbh_dst if rotate argument is true
# data to be pulled are defined by col_defs
# records to be pulled are defined by where clause
def pull_table(rotate, dbh_src, dbh_dst, table_src, col_defs, where='', table_dst=nil) # :nodoc:
  table_dst = table_src if table_dst.nil?
  puts "pulling #{table_src} to #{table_dst}"
  
  dbh_dst.execute("DROP TABLE IF EXISTS #{table_dst}_temp")
  dbh_dst.execute("CREATE TABLE #{table_dst}_temp (" +
    col_defs.collect { |col| "#{col[0]} #{col[1]}" }.join(',') + 
    ") ENGINE=MyISAM")  # MyISAM tables can be checked for update_time
  
  query = "SELECT " + col_defs.collect { |col| col[0] }.join(',') + " FROM #{table_src} " + where
  sth = dbh_src.execute(query)
  sth.fetch_hash do |row|
    # puts row.inspect
    dbh_dst.execute("INSERT INTO #{table_dst}_temp VALUES (" + 
    col_defs.collect { |col| dbh_dst.quote(row[col[0]]) }.join(',') + 
    ")")
  end
  sth.finish
  
  rotate_table(dbh_dst, table_dst) if rotate
  dbh_dst.execute("DROP TABLE IF EXISTS #{table_dst}")
  dbh_dst.execute("RENAME TABLE #{table_dst}_temp TO #{table_dst}")
end

# grab all the powerschool data we're interested in
def pull_and_rotate_all_tables(dbh_src, dbh_dst)
  pull_table(true, dbh_src, dbh_dst, 'schools',  School_col_defs)
  pull_table(true, dbh_src, dbh_dst, 'students', Student_col_defs) # 'WHERE enroll_status <= 0'
  pull_table(true, dbh_src, dbh_dst, 'teachers', Teacher_col_defs) # 'WHERE status = 1'
  pull_table(true, dbh_src, dbh_dst, 'terms',    Term_col_defs)
  pull_table(true, dbh_src, dbh_dst, 'gradescaleitem', Gradescale_col_defs,     'WHERE gradescaleid = -1',  'gradescales')
  pull_table(true, dbh_src, dbh_dst, 'gradescaleitem', Gradescaleitem_col_defs, 'WHERE gradescaleid <> -1', 'gradescaleitems')
  pull_table(true, dbh_src, dbh_dst, 'courses',  Course_col_defs)
  pull_table(true, dbh_src, dbh_dst, 'sections', Section_col_defs)
  pull_table(true, dbh_src, dbh_dst, 'cc',       Cc_col_defs, 'WHERE termid >= 1700 AND termid < 1800')
end

# wrapper to send an email notice
# uses TMail class (included in Rails action_mailer)
def send_email_notification(subject, msgline, t1, t2, changes)
  body = "#{msgline}\ntime: #{t1}\nprev: #{t2}\n"
  if changes
    body << "updates:\n"
    changes.each do |col, v_new, v_old|
      body << "  #{col} now '#{v_new}', was '#{v_old}'\n"
    end
  end
  mail = TMail::Mail.new
  mail.to      = APP_CONFIG[:mail_to]
  mail.from    = APP_CONFIG[:mail_from]
  mail.subject = subject
  mail.date    = Time.now
  mail.mime_version = '1.0'
  mail.set_content_type 'text', 'plain'
  mail.body    = body
  smtp = Net::SMTP.new(APP_CONFIG[:mail_server], APP_CONFIG[:mail_port])
  smtp.start(APP_CONFIG[:mail_server], 
    APP_CONFIG[:mail_user], APP_CONFIG[:mail_password], APP_CONFIG[:mail_auth]) do
    smtp.send_mail(mail.encoded, mail.from[0], mail.to)
  end
  print "sent #{msgline}\n"
end

# gets tables, does a diff, and sends emails notification of changes found
def pull_diff_and_notify(dbh_src, dbh_dst)
  pull_table(false, dbh_src, dbh_dst, 'students', Student_col_defs) # 'WHERE enroll_status <= 0'
  print "students table data pulled\n"
  change_count = 0
  diff_table(dbh_dst, 'students', 
    Student_diff_cols, Student_diff_keys) do |change_type, id, t1, t2, changes|
    send_email_notification('student db change', "student #{id} #{change_type}", t1, t2, changes)
    change_count += 1
  end
  print "students table data diffed: #{change_count} changes\n"
  pull_table(false, dbh_src, dbh_dst, 'cc', Cc_col_defs, 'WHERE termid >= 1700 AND termid < 1800')
  print "cc table data pulled\n"
  change_count = 0
  diff_table(dbh_dst, 'cc', 
    Cc_diff_cols, Cc_diff_keys) do |change_type, id, t1, t2, changes|
    send_email_notification('enrollment db change',  "enrollment #{id} #{change_type}", t1, t2, changes)
    change_count += 1
  end
  print "cc table data diffed: #{change_count} changes\n"
end 

# returns two DBI connection handles.  
# dbh_src is the ODBC connection to the source PowerSchool database that 
# data is pulled from.  
# dbh_dst is a connection to a MySQL database that the data is copied to, 
# and in which the comparator tables are built and queried.
def open_connections
  dbh_src = nil
  dsn_src = "dbi:#{APP_CONFIG[:dbi_src][:adapter]}:#{APP_CONFIG[:dbi_src][:database]}"
  dbh_dst = nil
  dsn_dst = "dbi:#{APP_CONFIG[:dbi_dst][:adapter]}:#{APP_CONFIG[:dbi_dst][:database]}"
  begin
    dbh_src = DBI.connect(dsn_src, APP_CONFIG[:dbi_src][:user], APP_CONFIG[:dbi_src][:password])
    begin
      dbh_dst = DBI.connect(dsn_dst, APP_CONFIG[:dbi_dst][:user], APP_CONFIG[:dbi_dst][:password])
    rescue
      $stderr.print "could not open #{dsn_dst}: #{$!}\n"
      exit
    end
  rescue
    $stderr.print "could not open #{dsn_src}: #{$!}\n"
    exit
  end
  [dbh_src, dbh_dst]
end

# main routine
dbh_src, dbh_dst = open_connections
pull_diff_and_notify(dbh_src, dbh_dst)


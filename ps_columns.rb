# ps_columns.rb

# PowerSchool ODBC column definitions
module PowerSchool

  # Schools table
  School_col_defs = [
    [ 'id',             'int not null' ],
    [ 'school_number',  'int not null' ],
    [ 'name',           'varchar(60) not null' ],
    [ 'low_grade',      'int not null' ],
    [ 'high_grade',     'int not null' ],
    [ 'alternate_school_number', 'int null' ],
  ]

  # Students table
  Student_col_defs = [
    [ 'id',             'int not null' ],
    [ 'schoolid',       'int not null' ],
    [ 'student_number', 'int not null' ],  # float in PowerSchool
    [ 'first_name',     'varchar(20) null' ],
    [ 'middle_name',    'varchar(20) null' ],
    [ 'last_name',      'varchar(30) null' ],
    [ 'enroll_status',  'int not null' ],
    [ 'entrydate',      'date not null' ],
    [ 'entrycode',      'varchar(10) null' ],
    [ 'exitdate',       'date not null' ],
    [ 'exitcode',       'varchar(10) null' ],
    [ 'gender',         'varchar(2) null'],
    [ 'grade_level',    'int not null' ],
    [ 'home_room',      'varchar(60) null' ]
  ]

  Student_diff_keys = 'id'
  Student_diff_cols = 'student_number,schoolid,grade_level,enroll_status,entrydate,entrycode,exitdate,exitcode'

  # Teachers table
  Teacher_col_defs = [
    [ 'id',             'int not null' ],
    [ 'schoolid',       'int not null' ],
    [ 'teachernumber',  'int not null' ],  # float in PowerSchool
    [ 'first_name',     'varchar(20) null' ],
    [ 'middle_name',    'varchar(20) null' ],
    [ 'last_name',      'varchar(20) null' ],
    [ 'preferredname',  'varchar(45) null' ],
    [ 'status',         'int not null' ],
    [ 'staffstatus',    'int not null' ],
  ]

  # Note that Gradescaleitem table contains grade *scales* as well as 
  # their component grade *items*
  # Gradescaleitem table - the grade scales
  Gradescale_col_defs = [
    [ 'id',             'int not null' ],
    [ 'name',           'varchar(50) null' ],
    [ 'description',    'varchar(255) not null' ],   # PowerSchool text
  ]

  # Gradescaleitem table - the items
  Gradescaleitem_col_defs = [
    [ 'id',             'int not null' ],
    [ 'gradescaleid',   'int not null' ],
    [ 'name',           'varchar(50) null' ],
    [ 'description',    'varchar(255) not null' ],   # PowerSchool text
    [ 'grade_points',   'float not null' ],
    [ 'cutoffpercentage', 'float not null' ],
  ]

  # Courses table
  Course_col_defs = [
    [ 'id',             'int not null' ],
    [ 'schoolid',       'int not null' ],
    [ 'course_number',  'varchar(11) not null' ],
    [ 'course_name',    'varchar(40) not null' ],
    [ 'credit_hours',   'float not null' ],
    [ 'credittype',     'varchar(20) null' ]
  ]

  # Sections table
  Section_col_defs = [
    [ 'id',             'int not null' ],
    [ 'schoolid',       'int not null' ],
    [ 'gradescaleid',   'int not null' ],
    [ 'course_number',  'varchar(11) not null' ],
    [ 'section_number', 'varchar(10) not null' ],
    [ 'expression',     'varchar(80) null' ],
    [ 'excludefromgpa', 'tinyint not null' ]
  ]

  # Terms table
  Term_col_defs = [
    [ 'id',             'int not null' ],
    [ 'schoolid',       'int not null' ],
    [ 'yearid',         'int not null' ],
    [ 'name',           'varchar(30) not null' ],
    [ 'abbreviation',   'varchar(6) not null' ],
    [ 'firstday',       'date not null' ],
    [ 'lastday',        'date not null' ]
  ]

  # CC table - course enrollments
  Cc_col_defs = [
    [ 'id',             'int not null' ],
    [ 'schoolid',       'int not null' ],
    [ 'studentid',      'int not null' ],
    [ 'teacherid',      'int not null' ],
    [ 'sectionid',      'int not null' ],
    [ 'termid',         'int not null' ],
    [ 'course_number',  'varchar(11) not null' ],
    [ 'section_number', 'varchar(10) not null' ],
    [ 'dateenrolled',   'date not null' ],
    [ 'dateleft',       'date not null' ]
  ]

  Cc_diff_keys = 'id'
  Cc_diff_cols = 'studentid,sectionid,termid,dateenrolled,dateleft'
end

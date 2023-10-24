CREATE TABLE IF NOT EXISTS students (
name STRING ,
id INT ,
subjects ARRAY < STRING >,
feeDetails MAP < STRING , FLOAT >,
phoneNumber STRUCT <areacode: INT , number : INT > )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '#'
MAP KEYS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Syntax:
-- load data inpath 'add path to your file here' overwrite into table <table_Name>;

-- Query:
load data local inpath '/opt/student.dat' overwrite into table students;

-- To check the output

Select * FROM students;

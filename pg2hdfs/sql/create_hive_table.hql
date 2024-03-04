CREATE DATABASE IF NOT EXISTS nineinfra;
use nineinfra;
CREATE TABLE employees
(
  emp_id int,
  first_name string,
  last_name string,
  hire_date date,
  job_id string ,
  salary DECIMAL(7,2) ,
  commission_pct DECIMAL(6,2) ,
  manager_id int,
  dept_id int
)
ROW FORMAT DELIMITED fields terminated BY ','
COLLECTION items terminated BY '_'
LINES terminated BY '\n'
TBLPROPERTIES("skip.header.line.count" = "1") ;

load data inpath '/nineinfra/datahouse/ods/2024.03.04_0.txt' INTO TABLE `nineinfra`.`employees`;
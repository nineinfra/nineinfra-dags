CREATE database IF NOT EXISTS nineinfra;
use nineinfra;
CREATE TABLE IF NOT EXISTS `nineinfra`.`employees`
(
  emp_id int auto_increment PRIMARY KEY,
  first_name VARCHAR(500) NOT NULL,
  last_name VARCHAR(500) NOT NULL,
  hire_date date,
  job_id VARCHAR(225) ,
  salary DECIMAL(7,2) ,
  commission_pct DECIMAL(6,2) ,
  manager_id int,
  dept_id int
)

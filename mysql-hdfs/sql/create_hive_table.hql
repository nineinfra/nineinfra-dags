CREATE DATABASE nineinfra;
use nineinfra;
CREATE TABLE authors
(
  id int,
  author_name string,
  country string
)
ROW FORMAT DELIMITED fields terminated BY ',' TBLPROPERTIES("skip.header.line.count" = "1") ;
CREATE TABLE books
(
  id int,
  book_title string
)
ROW FORMAT DELIMITED fields terminated BY ',' TBLPROPERTIES("skip.header.line.count" = "1") ;
load data local inpath '/nineinfra/datahouse/ods/data_authors.txt' INTO TABLE `nineinfra`.`authors`;
load data local inpath '/nineinfra/datahouse/ods/data_books.txt' INTO TABLE `nineinfra`.`books`
CREATE DATABASE hivedemo;
SET hive.support.concurrency=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

use hivedemo;
create table products (PID int, type string, city string, customer_code string, description string, time_stamp timestamp) clustered by (city) into 3 buckets row format delimited fields terminated by ',' lines terminated by '\n' stored as orc TBLPROPERTIES ('transactional'='true');
insert into products (pid,type,city,customer_code,description,time_stamp) values 
(2,'stationary','mumbai','fgdh5472','book',current_timestamp());
insert into products (pid,type,city,customer_code,description,time_stamp) values 
(1,'stationary','mumbai','fgdh5472','book',current_timestamp());
insert into products (pid,type,city,customer_code,description,time_stamp) values 
(3,'stationary','mumbai','fgdh5472','book',current_timestamp());

#stg
CREATE EXTERNAL TABLE dlybin.stg_billing (user_id int, billing_period string, service string, tariff string, sum string, created_at string)

CREATE EXTERNAL TABLE dlybin.stg_issue (user_id string, start_time string, end_time string, title string, description string, service string)

CREATE EXTERNAL TABLE dlybin.stg_payment (user_id int, pay_doc_type string, pay_doc_num string, account string,phone string, billing_period string,
 pay_date string, sum string)

CREATE EXTERNAL TABLE dlybin.stg_traffic (user_id int,timestamp string, device_id string, device_ip_addr string, bytes_sent string, bytes_received string)

#ods
CREATE EXTERNAL TABLE dlybin.ods_billing (user_id int, billing_period int, service string, tariff string, sum decimal(20,2), created_at timestamp)
PARTITIONED BY (year int)

CREATE EXTERNAL TABLE dlybin.ods_issue(user_id int, start_time timestamp, end_time timestamp, title string, description string, service string) 
PARTITIONED BY (year int)

CREATE EXTERNAL TABLE dlybin.ods_payment(user_id int, pay_doc_type string, pay_doc_num int, account string, phone string, billing_period int, pay_date timestamp,
  sum decimal(20,2)) PARTITIONED BY (year int)

CREATE EXTERNAL TABLE dlybin.ods_traffic(user_id int, event_ts timestamp, device_id string, device_ip_addr string, bytes_sent int, bytes_received int)
PARTITIONED BY (year int)

#dm
CREATE EXTERNAL TABLE dlybin.dm_traffic(user_id int, max_bytes_received int, min_bytes_received int, avg_bytes_received float, event_year int)
PARTITIONED BY (year int)













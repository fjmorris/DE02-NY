2.2.5 Data Exploration
Credit Card System Req-2.2.5 Data exploration
Functional Requirements
Use Pig
1) Find the top 20 zip codes(hint: branch_zip) by total transaction value

--https://stackoverflow.com/questions/27765616/how-to-find-average-after-joining-two-datasets-and-grouping-in-pig
******************

[root@sandbox ~]# pig

grunt> branch = LOAD '/user/maria_dev/cdw_sapp/branch/part-m-00000' USING PigStorage(',') as (BRANCH_CODE:int, BRANCH_NAME:chararray, BRANCH_STREET:chararray, BRANCH_CITY:chararray, BRANCH_STATE:chararray, BRANCH_ZIP:int, FORMATTED_PHONE:chararray, LAST_UPDATED:chararray);

grunt> credit_card = LOAD '/user/maria_dev/cdw_sapp/credit_card/part-m-00000' USING PigStorage(',') as (TRANSACTION_ID:int, CREDIT_CARD_NO:chararray, TIMEID:chararray, CUST_SSN:int, BRANCH_CODE:int, TRANSACTION_TYPE:chararray, TRANSACTION_VALUE:double);

grunt> join_data = JOIN branch by BRANCH_CODE, credit_card by BRANCH_CODE;

grunt> group_data = GROUP join_data by BRANCH_ZIP;

grunt> limit_data = LIMIT group_data 4; 

grunt> group_sum = FOREACH group_data GENERATE group, SUM(join_data.credit_card::TRANSACTION_VALUE) as total;

grunt> order_by_data = ORDER group_sum BY total DESC;

grunt> limit_data = LIMIT order_by_data 20; 

grunt> Dump limit_data;


2018-06-14 18:42:14,535 [main] INFO  org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil - Total input paths to process : 1
(2155,23792.889999999996)
(52722,23641.99000000001)
(11756,23507.66)
(60091,23350.679999999993)
(33904,22991.329999999994)
(30236,22895.510000000006)
(71730,22772.800000000017)
(11803,22766.419999999987)
(7501,22727.540000000005)
(27834,22675.209999999995)
(91010,22583.27)
(38655,22500.280000000017)
(29550,22438.220000000005)
(48047,22401.719999999983)
(53151,22384.680000000008)
(27284,22377.30999999998)
(15317,22365.060000000012)
(49418,22357.390000000014)
(98444,22356.679999999982)
(32703,22247.959999999992)
grunt>


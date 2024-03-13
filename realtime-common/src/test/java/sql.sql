select *
from topic_db
where `database` = 'gmall'
  and `table` = 'comment_info';


CREATE TABLE MyUserTable
(
    id     BIGINT,
    name   STRING,
    age    INT,
    status BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/mydatabase',
    'table-name' = 'users'
);

-- register the HBase table 'mytable' in Flink SQL
CREATE TABLE base_dic
(
    rowkey STRING,
    info ROW <dic_name STRING >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'gmall:dim_base_dic',
 'zookeeper.quorum' = 'localhost:2181'
);

-- 读取流中的评论信息表
select `data`['id']           id,
       `data`['user_id']      user_id,
       `data`['nick_name']    nick_name,
       `data`['head_img']     head_img,
       `data`['sku_id']       sku_id,
       `data`['spu_id']       spu_id,
       `data`['order_id']     order_id,
       `data`['appraise']     appraise,
       `data`['comment_txt']  comment_txt,
       `data`['create_time']  comment_time,
       `data`['operate_time'] operate_time,
       proc_time
from topic_db
where `database` = 'gmall'
  and `table` = 'comment_info'
  and `type` = 'insert';

-- 读取维度表进行维度退化
select id,
       user_id,
       nick_name,
       head_img,
       sku_id,
       spu_id,
       order_id,
       appraise appraise_code,
       info.dic_name appraise_name,
       comment_txt,
       create_time,
       operate_time,
       proc_time
from comment_info c
join base_dic FOR SYSTEM_TIME AS OF c.proc_time as b
on c.appraise = b.rowkey;

id STRING,
user_id STRING,
nick_name STRING,
sku_id STRING,
spu_id STRING,
order_id STRING,
appraise_code STRING,
appraise_name STRING,
comment_txt STRING,
create_time STRING,
operate_time STRING
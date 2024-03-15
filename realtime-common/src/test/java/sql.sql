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
       appraise      appraise_code,
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
    
    
    ;
    
-- 交易域加购
select `data`['id']                                                             id,
       `data`['user_id']                                                        user_id,
       `data`['sku_id']                                                         sku_id,
       `data`['cart_price']                                                     cart_price,
       if(`type` = 'insert', cast(`data`['sku_num']),
          cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint)) sku_num,
       `data`['sku_name']                                                       sku_name,
       `data`['is_checked']                                                     is_checked,
       `data`['create_time']                                                    create_time,
       `data`['operate_time']                                                   operate_time,
       `data`['is_ordered']                                                     is_ordered,
       `data`['order_time']                                                     order_time,
       ts
from topic_db
where `database` = 'gmall'
  and `table` = 'cart_info'
  and (`type` = 'insert'
    or (`type` = 'update'
        and `old`['sku_num'] is not null
        and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint))
    );

create table dwd_trade_cart_add
(
    id           STRING,
    user_id      STRING,
    sku_id       STRING,
    cart_price   STRING,
    sku_num      bigint,
    sku_name     STRING,
    is_checked   STRING,
    create_time  STRING,
    operate_time STRING,
    is_ordered   STRING,
    order_time   STRING,
    ts           bigint
)


-- dwd-下单

select `data`['id']                    id,
       `data`['order_id']              order_id,
       `data`['sku_id']                sku_id,
       `data`['sku_name']              sku_name,
       `data`['order_price']           order_price,
       `data`['sku_num']               sku_num,
       `data`['create_time']           create_time,
       `data`['split_total_amount']    split_total_amount,
       `data`['split_activity_amount'] split_activity_amount,
       `data`['split_coupon_amount']   split_coupon_amount,
       `data`['operate_time']          operate_time,
       ts
from topic_db
where `database` = 'gmall'
  and `table` = 'order_detail'
  and `type` = 'insert'
;


select `data`['id']          id,
       `data`['user_id']     user_id,
       `data`['province_id'] province_id
from topic_db
where `database` = 'gmall'
  and `table` = 'order_info'
  and `type` = 'insert';

select od.id,
       od.order_id,
       od.sku_id,
       od.sku_name,
       od.order_price,
       od.sku_num,
       od.create_time,
       od.split_total_amount,
       od.split_activity_amount,
       od.split_coupon_amount,
       od.operate_time,
       oi.user_id,
       oi.province_id,
       oda.activity_id,
       oda.activity_rule_id,
       odc.coupon_id,
       od.ts
from order_detail od
         join order_info oi on od.order_id = oi.id
         left join order_detail_activity oda on od.id = oda.order_detail_id
         left join order_detail_coupon odc on od.id = odc.order_detail_id;


--
select `data`['order_detail_id']  order_detail_id,
       `data`['activity_id']      activity_id,
       `data`['activity_rule_id'] activity_rule_id
from topic_db
where `database` = 'gmall'
  and `table` = 'order_detail_activity'
  and `type` = 'insert';


--
select `data`['order_detail_id'] order_detail_id,
       `data`['coupon_id']       coupon_id
from topic_db
where `database` = 'gmall'
  and `table` = 'order_detail_coupon'
  and `type` = 'insert'
;


-- kafkaSink表映射
create table dwd_trade_order_detail
(
    id                    STRING,
    order_id              STRING,
    sku_id                STRING,
    sku_name              STRING,
    order_price           STRING,
    sku_num               STRING,
    create_time           STRING,
    split_total_amount    STRING,
    split_activity_amount STRING,
    split_coupon_amount   STRING,
    operate_time          STRING,
    user_id               STRING,
    province_id           STRING,
    activity_id           STRING,
    activity_rule_id      STRING,
    coupon_id             STRING,
    ts                    bigint
)

select od.id,
       od.order_id,
       od.sku_id,
       od.sku_name,
       od.order_price,
       od.sku_num,
       od.create_time,
       od.split_total_amount,
       od.split_activity_amount,
       od.split_coupon_amount,
       ct.operate_time,
       od.user_id,
       od.province_id,
       od.activity_id,
       od.activity_rule_id,
       od.coupon_id,
       ct.ts
from cancel_table ct
         join dwd_trade_order_detail od
              on ct.id = od.order_id;



select `data`['id']           id,
       `data`['operate_time'] operate_time,
       `data`['province_id']  province_id,
       ts
from topic_db
where `database` = 'gmall'
  and `table` = 'order_info'
  and `type` = 'update'
  and cast(`old`['order_status'] as STRING) = '1001'
  and cast(`data`['order_status'] as STRING) = '1003';


create table dwd_trade_order_cancel
(
    id                    STRING,
    order_id              STRING,
    sku_id                STRING,
    sku_name              STRING,
    order_price           STRING,
    sku_num               STRING,
    create_time           STRING,
    split_total_amount    STRING,
    split_activity_amount STRING,
    split_coupon_amount   STRING,
    operate_time          STRING,
    user_id               STRING,
    province_id           STRING,
    activity_id           STRING,
    activity_rule_id      STRING,
    coupon_id             STRING,
    ts                    bigint
);



select `data`['user_id']       user_id,
       `data`['order_id']      order_id,
       `data`['payment_type']  payment_type,
       `data`['callback_time'] callback_time,
       `pt`,
       ts,
       et
from topic_db
where `database` = 'gmall'
  and `table` = 'payment_info'
  and `type` = 'update'
  and `old`['payment_status'] is not null
  and `data`['payment_status'] = '1602';


select od.id           order_detail_id,
       od.order_id,
       od.user_id,
       od.sku_id,
       od.sku_name,
       od.province_id,
       od.activity_id,
       od.activity_rule_id,
       od.coupon_id,
       pi.payment_type payment_type_code,
       dic.dic_name    payment_type_name,
       pi.callback_time,
       od.sku_num,
       od.order_price,
       od.split_activity_amount,
       od.split_coupon_amount,
       od.split_total_amount,
       pi.ts
from payment_info pi
         join dwd_trade_order_detail od
              on pi.order_id = od.order_id
                  and od.et >= pi.et - interval '30' minute
                  and od.et <= pi.et + interval '5' second
         join base_dic for system_time as of pi.pt as dic
on pi.payment_type = dic.dic_code;


create table xxx
(
    id                    STRING,
    order_id              STRING,
    user_id               STRING,
    sku_id                STRING,
    sku_name              STRING,
    province_id           STRING,
    activity_id           STRING,
    activity_rule_id      STRING,
    coupon_id             STRING,
    payment_type          STRING,
    dic_name              STRING,
    callback_time         STRING,
    sku_num               STRING,
    order_price           STRING,
    split_activity_amount STRING,
    split_coupon_amount   STRING,
    split_total_amount    STRING,
    ts                    bigint,
    primary key (id) not enforced
);



create table page_log
(
    page map<string,string>,
    ts   bigint,
    row_time as to_timestamp_ltz(ts,3),
    watermark for row_time as row_time - interval '5' second
)
with ()

select page['item'] keyword,
       row_time
from page_log
where (page['last_page_id'] = 'search'
    or page['last_page_id'] = 'home')
  and page['item_type'] = 'keyword'
  and page['item'] is not null;


select key_word,
       row_time
from keywords_table
         join lateral table (kw_split(keyword)) on true;


select TUMBLE_START(row_time, INTERVAL '10' SECOND) as stt,
       TUMBLE_END(row_time, INTERVAL '10' SECOND)   as edt,
       CURRENT_DATE                                    cur_date,
       keyword,
       count(*)                                        keyword_count
from keyword_table
GROUP BY TUMBLE(row_time, INTERVAL '10' SECOND),
         keyword
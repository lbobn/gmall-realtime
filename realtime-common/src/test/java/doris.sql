
-- 用户注册
drop table if exists gmall2023_realtime.dws_user_user_register_window;
create table if not exists gmall2023_realtime.dws_user_user_register_window
(
    `stt`         DATETIME COMMENT '窗口起始时间',
    `edt`         DATETIME COMMENT '窗口结束时间',
    `cur_date`    DATE COMMENT '当天日期',
    `register_ct` BIGINT REPLACE COMMENT '注册用户数'
) engine = olap aggregate key (`stt`,`edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);

-- 加购
drop table if exists gmall2023_realtime.dws_trade_cart_add_uu_window;
create table if not exists gmall2023_realtime.dws_trade_cart_add_uu_window
(
    `stt`            DATETIME COMMENT '窗口起始时间',
    `edt`            DATETIME COMMENT '窗口结束时间',
    `cur_date`       DATE COMMENT '当天日期',
    `cart_add_uu_ct` BIGINT REPLACE COMMENT '加购独立用户数'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);

--
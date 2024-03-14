package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 *
 *
 *
 * @description  交易域下单事务事实表
 * @Author lubb
 * @create 2024-03-14 15:50
 */
public class DwdTradeOrderDetail extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(
                10014,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        // 核心业务处理，因为需要表join,所以需要设置ttl
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        // 1. 读取数据
        createTopicDB(ckAndGroupId,tableEnv);

        // 2. 过滤出订单详情表
        filterOD(tableEnv);

        // 3. 过滤出订单信息表
        filterOI(tableEnv);

        // 4. 过滤出活动表
        filterODA(tableEnv);

        // 5. 过滤出优惠券表
        filterODC(tableEnv);

        // 6. 进行join
        Table joinedTable = getJoinedTable(tableEnv);

        // 7. 写出到kafka

        // 创建表映射
        createUpsertKafkaSinkTable(tableEnv);

        joinedTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();

    }

    private static void createUpsertKafkaSinkTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"\n" +
                "(\n" +
                "    id                    STRING,\n" +
                "    order_id              STRING,\n" +
                "    sku_id                STRING,\n" +
                "    sku_name              STRING,\n" +
                "    order_price           STRING,\n" +
                "    sku_num               STRING,\n" +
                "    create_time           STRING,\n" +
                "    split_total_amount    STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount   STRING,\n" +
                "    operate_time          STRING,\n" +
                "    user_id               STRING,\n" +
                "    province_id           STRING,\n" +
                "    activity_id           STRING,\n" +
                "    activity_rule_id      STRING,\n" +
                "    coupon_id             STRING,\n" +
                "    ts                    bigint," +
                "    primary key(id) not enforced\n" +
                ")" + SQLUtil.getUpsertKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    private static Table getJoinedTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select od.id,\n" +
                "       od.order_id,\n" +
                "       od.sku_id,\n" +
                "       od.sku_name,\n" +
                "       od.order_price,\n" +
                "       od.sku_num,\n" +
                "       od.create_time,\n" +
                "       od.split_total_amount,\n" +
                "       od.split_activity_amount,\n" +
                "       od.split_coupon_amount,\n" +
                "       od.operate_time,\n" +
                "       oi.user_id,\n" +
                "       oi.province_id,\n" +
                "       oda.activity_id,\n" +
                "       oda.activity_rule_id,\n" +
                "       odc.coupon_id,\n" +
                "       od.ts\n" +
                "from order_detail od\n" +
                "         join order_info oi on od.order_id = oi.id\n" +
                "         left join order_detail_activity oda on od.id = oda.order_detail_id\n" +
                "         left join order_detail_coupon odc on od.id = odc.order_detail_id");
    }

    private static void filterODC(StreamTableEnvironment tableEnv) {
        Table orderDetailCoupon = tableEnv.sqlQuery("select \n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['coupon_id']       coupon_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "  and `table` = 'order_detail_coupon'\n" +
                "  and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
    }

    private static void filterODA(StreamTableEnvironment tableEnv) {
        Table orderDetailActivity = tableEnv.sqlQuery("select " +
                "       `data`['order_detail_id']  order_detail_id,\n" +
                "       `data`['activity_id']      activity_id,\n" +
                "       `data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "  and `table` = 'order_detail_activity'\n" +
                "  and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
    }

    private static void filterOI(StreamTableEnvironment tableEnv) {
        Table orderInfo = tableEnv.sqlQuery("select \n" +
                "    `data`['id']          id,\n" +
                "    `data`['user_id']     user_id,\n" +
                "    `data`['province_id'] province_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "  and `table` = 'order_info'\n" +
                "  and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info", orderInfo);
    }

    private static void filterOD(StreamTableEnvironment tableEnv) {
        Table orderDetail = tableEnv.sqlQuery("select `data`['id']                    id,\n" +
                "       `data`['order_id']              order_id,\n" +
                "       `data`['sku_id']                sku_id,\n" +
                "       `data`['sku_name']              sku_name,\n" +
                "       `data`['order_price']           order_price,\n" +
                "       `data`['sku_num']               sku_num,\n" +
                "       `data`['create_time']           create_time,\n" +
                "       `data`['split_total_amount']    split_total_amount,\n" +
                "       `data`['split_activity_amount'] split_activity_amount,\n" +
                "       `data`['split_coupon_amount']   split_coupon_amount,\n" +
                "       `data`['operate_time']          operate_time,\n" +
                "       ts\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "  and `table` = 'order_detail'\n" +
                "  and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail", orderDetail);
    }
}

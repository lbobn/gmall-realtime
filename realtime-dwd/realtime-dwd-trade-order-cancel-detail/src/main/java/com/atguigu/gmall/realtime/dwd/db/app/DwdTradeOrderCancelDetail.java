package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @description 交易域取消订单事实表
 *              注：运行该程序时需要先启动DwdTradeOrderDetail程序，
 *              因为该程序会向Kafka中写入数据，所以DwdTradeOrderCancelDetail程序会读取这些数据。
 * @Author lubb
 * @create 2024-03-14 17:06
 */
public class DwdTradeOrderCancelDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(
                10015,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_CANCEL
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        // 设计join，设置ttl 30 min+5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 处理逻辑
        // 1. 读取topic
        createTopicDB(ckAndGroupId, tableEnv);
        // 2. 过滤订单信息表，type=update，且修改的是支付状态改为取消
        Table cancelTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id']          id,\n" +
                "   `data`['operate_time'] operate_time,\n" +
                "    `data`['province_id'] province_id," +
//                "   `data`['order_status'] order_status," +
//                "   `old`['order_status'] old_order_status," +
                "       ts      \n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "  and `table` = 'order_info'\n" +
                "  and `type` = 'update'" +
                "and `old`['order_status'] = '1001'" +
                "and `data`['order_status'] = '1003'");
        tableEnv.createTemporaryView("cancel_table", cancelTable);

        /* 可以直接读取dwd下单表再次join，也可以重新读取各个表再join */
        // 3. 过滤订单信息表
        // 4. 过滤订单活动表
        // 5. 过滤订单优惠信息表
        // 读取Dwd 层下单表
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "\n" +
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
                "    ts                    bigint" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        // 6. join
        Table joinedTable = tableEnv.sqlQuery("select\n" +
                "    od.id                    ,\n" +
                "    od.order_id              ,\n" +
                "    od.sku_id                ,\n" +
                "    od.sku_name              ,\n" +
                "    od.order_price           ,\n" +
                "    od.sku_num               ,\n" +
                "    od.create_time           ,\n" +
                "    od.split_total_amount    ,\n" +
                "    od.split_activity_amount ,\n" +
                "    od.split_coupon_amount   ,\n" +
                "    ct.operate_time          ,\n" +
                "    od.user_id               ,\n" +
                "    od.province_id           ,\n" +
                "    od.activity_id           ,\n" +
                "    od.activity_rule_id      ,\n" +
                "    od.coupon_id             ,\n" +
                "    ct.ts\n" +
                "from cancel_table ct\n" +
                "         join dwd_trade_order_detail od\n" +
                "              on ct.id = od.order_id");

        // 7. 写出kafka

        // 创建sink映射表
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + "\n" +
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
                "    ts                    bigint\n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        joinedTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL).execute();
    }
}

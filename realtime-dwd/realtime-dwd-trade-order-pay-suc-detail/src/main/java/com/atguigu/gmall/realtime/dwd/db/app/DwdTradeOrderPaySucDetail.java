package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description 支付成功事务事实表
 * @Author lubb
 * @create 2024-03-14 21:49
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(
                10016,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        // 主要实现支付成功事实表
        /*   由于要使用interval join ,而他则需要事件事件，即水位线
         *   而且要使用lookup join 来连接字典表*/
        // 1. 读取dwd下单事实表
        getDWDOrderDetail(tableEnv);
//        tableEnv.executeSql("select * from " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).print();

        // 2. 读取字典表
        // 字典为维度表，存储在HBase中，故读取HBase数据
        createBaseDic(tableEnv);
//        tableEnv.executeSql("select * from base_dic").print();


        // 3. 读取topic_db
        createTopicDB(ckAndGroupId, tableEnv);

        // 4. 从topic_db中过滤支付成功的信息表
        Table paymentInfo = tableEnv.sqlQuery("select `data`['user_id'] user_id,\n" +
                "       `data`['order_id'] order_id,\n" +
                "       `data`['payment_type'] payment_type,\n" +
                "       `data`['callback_time'] callback_time,\n" +
                "       proc_time,\n" +
                "       ts,\n" +
                "       row_time\n" +

                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "  and `table` = 'payment_info'\n" +
                "  and `type` = 'update'\n" +
                "  and `old`['payment_status'] is not null\n" +
                "  and `data`['payment_status'] = '1602'");
        // 注册为视图供后续使用
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // 5. 表连接
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "    od.id order_detail_id,\n" +
                "    od.order_id,\n" +
                "    od.user_id,\n" +
                "    od.sku_id,\n" +
                "    od.sku_name,\n" +
                "    od.province_id,\n" +
                "    od.activity_id,\n" +
                "    od.activity_rule_id,\n" +
                "    od.coupon_id,\n" +
                "    pi.payment_type payment_type_code,\n" +
                "    dic.dic_name payment_type_name,\n" +
                "    pi.callback_time,\n" +
                "    od.sku_num,\n" +
                "    od.order_price,\n" +
                "    od.split_activity_amount,\n" +
                "    od.split_coupon_amount,\n" +
                "    od.split_total_amount,\n" +
                "    pi.ts\n" +
                "from payment_info pi\n" +
                "         join dwd_trade_order_detail od\n" +
                "              on pi.order_id = od.order_id\n" +
                "                  and od.row_time >= pi.row_time - interval '30' minute\n" +
                "                  and od.row_time <= pi.row_time + interval '5' second\n" +
                "join base_dic for system_time as of pi.proc_time as dic\n" +
                "on pi.payment_type = dic.rowkey");

        // 6. 输出到kafka
        // 使用upsertKakfa
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "\n" +
                "(\n" +
                "    id                    STRING,\n" +
                "    order_id              STRING,\n" +
                "    user_id               STRING,\n" +
                "    sku_id                STRING,\n" +
                "    sku_name              STRING,\n" +
                "    province_id           STRING,\n" +
                "    activity_id           STRING,\n" +
                "    activity_rule_id      STRING,\n" +
                "    coupon_id             STRING,\n" +
                "    payment_type          STRING,\n" +
                "    dic_name              STRING,\n" +
                "    callback_time         STRING,\n" +
                "    sku_num               STRING,\n" +
                "    order_price           STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount   STRING,\n" +
                "    split_total_amount    STRING,\n" +
                "    ts                    bigint,\n" +
                "    primary key (id) not enforced\n" +
                ")" + SQLUtil.getUpsertKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();

    }

    private static void getDWDOrderDetail(StreamTableEnvironment tableEnv) {
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
                "    ts                    bigint," +
                "    row_time as to_timestamp_ltz(ts, 3)," +   // 注册水位线需要timestamp(3),故将ts转为相应格式
                "    watermark for row_time as row_time - interval '3' second" +    // 注册水位线
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }
}

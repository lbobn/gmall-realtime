package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description
 * @Author lubb
 * @create 2024-03-14 10:32
 */
public class DwdTradeCartAdd extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }


    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        // 读取topic_db的数据
        // 1.创建表映射
        createTopicDB(Constant.TOPIC_DWD_TRADE_CART_ADD, tableEnv);


        // 2.过滤数据
        Table filterCartAdd = getFilterCartAdd(tableEnv);

        // 3. 写出到kafka对应主题
        // 创建KafkaSink表映射
        getTableKafkaSink(tableEnv);

        // 写出到kafka
        filterCartAdd.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();


    }

    private static TableResult getTableKafkaSink(StreamTableEnvironment tableEnv) {
        return tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                "    id STRING,\n" +
                "    user_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    cart_price STRING,\n" +
                "    sku_num bigint,\n" +
                "    sku_name STRING,\n" +
                "    is_checked STRING,\n" +
                "    create_time STRING,\n" +
                "    operate_time STRING,\n" +
                "    is_ordered STRING,\n" +
                "    order_time STRING,\n" +
                "    ts bigint\n" +
                "    )" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }

    private static Table getFilterCartAdd(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select `data`['id']                                                             id,\n" +
                "       `data`['user_id']                                                        user_id,\n" +
                "       `data`['sku_id']                                                         sku_id,\n" +
                "       `data`['cart_price']                                                     cart_price,\n" +
                "       if(`type` = 'insert', cast(`data`['sku_num'] as bigint),\n" +
                "          cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint)) sku_num,\n" +
                "       `data`['sku_name']                                                       sku_name,\n" +
                "       `data`['is_checked']                                                     is_checked,\n" +
                "       `data`['create_time']                                                    create_time,\n" +
                "       `data`['operate_time']                                                   operate_time,\n" +
                "       `data`['is_ordered']                                                     is_ordered,\n" +
                "       `data`['order_time']                                                     order_time,\n" +
                "       ts\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "  and `table` = 'cart_info'\n" +
                "  and (`type` = 'insert'\n" +
                "    or (`type` = 'update'\n" +
                "        and `old`['sku_num'] is not null\n" +
                "        and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint))\n" +
                "    )");
    }
}

package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * @description
 * @Author lubb
 * @create 2024-03-13 21:16
 */
public class SQLUtil {
    public static String getKafkaSourceSQL(String topicName, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getKafkaTopicDB(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `ts` bigint,\n" +
                " `type` STRING,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "  `proc_time` as PROCTIME()," +
                "  row_time as TO_TIMESTAMP_LTZ(ts,3)," +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ") " + getKafkaSourceSQL(Constant.TOPIC_DB, groupId);
    }

    public static String getKafkaSinkSQL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getUpsertKafkaSinkSQL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    public static String getDorisSink(String tableName) {
        return "with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + "." + tableName + "'," +
                "  'username' = 'root'," +
                "  'password' = 'aaaaaa', " +
                "   'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "'" +
                ")";
    }
}

package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;

/**
 * @Description
 * @Author lubb
 * @Time 2024-03-12 23:25
 */
public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topicName){
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchemaBuilder<String>()
                                .setTopic(topicName)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("atguigu-"+topicName+System.currentTimeMillis())
                .setProperty("transaction.timeout.ms",15*60*1000+"")
                .build();
    }
}

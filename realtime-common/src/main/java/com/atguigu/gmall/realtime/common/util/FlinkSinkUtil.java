package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

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

    public static KafkaSink<JSONObject> getKafkaSinkWithTopic(){
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<JSONObject>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(JSONObject s, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                String topicName = s.getString("sink_table");
                                s.remove("sink_table");
                                return new ProducerRecord<>(topicName,Bytes.toBytes(s.toJSONString()));
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("atguigu-"+"base-dwd"+System.currentTimeMillis())
                .setProperty("transaction.timeout.ms",15*60*1000+"")
                .build();
    }
}

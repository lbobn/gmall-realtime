package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSourceUtil {
    /**
     * 获取flink Kafka source
     * @param groupId
     * @param topicName
     * @return
     */
    public static KafkaSource<String> getKafkaSource(String groupId, String topicName) {
        return KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics(topicName)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setGroupId(groupId)
                .setValueOnlyDeserializer(

                        //无法反序列化空值，会报错
                        // 后续DWD会写入空值
//                        new SimpleStringSchema()
                        //通过匿名内部类实现
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if (bytes != null && bytes.length != 0) {
                                    return new String(bytes, StandardCharsets.UTF_8);
                                }
                                return "";
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return BasicTypeInfo.STRING_TYPE_INFO;
                            }
                        }
                )
                .build();
    }

    /**
     * 获取flink CDC mysql source
     * @param database
     * @param table
     * @return
     */
    public static MySqlSource<String> getMySqlSource(String database, String table) {
        Properties prop = new Properties();
        prop.setProperty("useSSL","false");
        prop.setProperty("allowPublicKeyRetrieval","true");

        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(prop)
                .databaseList(database)
                .tableList(database + "." + table)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
    }
}

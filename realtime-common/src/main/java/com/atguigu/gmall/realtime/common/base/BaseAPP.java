package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * 基类
 */
public abstract class BaseAPP {
    /**
     * 程序启动
     *
     * @param port         webUI端口
     * @param parallelism  并行度
     * @param ckAndGroupId 消费者组id
     * @param topicName    主题
     */
    public void start(int port, int parallelism, String ckAndGroupId, String topicName) {

        // 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);


        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//
//        // 1.4.2 开启 checkpoint
//        env.enableCheckpointing(5000);
//        // 1.4.3 设置 checkpoint 模式: 精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 1.4.4 checkpoint 存储
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/stream/" + ckAndGroupId);
//        // 1.4.5 checkpoint 并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 1.4.6 checkpoint 之间的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        // 1.4.7 checkpoint  的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        // 1.4.8 job 取消时 checkpoint 保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        //读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                FlinkSourceUtil.getKafkaSource(ckAndGroupId, topicName),
                WatermarkStrategy.noWatermarks(),
                "kafka_source"
        );


        /*              KafkaSource.<String>builder()
                        .setBootstrapServers("hadoop102:9092")
                        .setTopics(topicName)
                        .setStartingOffsets(OffsetsInitializer.earliest())    // 设置消费偏移量
                        .setGroupId(ckAndGroupId)
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(), WatermarkStrategy.noWatermarks(), "kafka_source"

        */
        //对数据源处理
        handle(env, kafkaSource);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 业务实现
     *
     * @param env    flink运行环境
     * @param stream 数据源
     */
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);
}

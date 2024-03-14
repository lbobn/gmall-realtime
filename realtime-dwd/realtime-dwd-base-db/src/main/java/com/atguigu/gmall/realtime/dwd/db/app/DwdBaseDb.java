package com.atguigu.gmall.realtime.dwd.db.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @description
 * @Author lubb
 * @create 2024-03-14 23:15
 */
public class DwdBaseDb extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseDb().start(
                10017,
                4,
                "dwd_base",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 处理逻辑，根据配置表动态将需要的表写入到kafka不同主题
        // 1. 对数据清洗，去除非json
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            System.out.println("过滤脏数据" + s);
                        }
                    }
                }
        );

        // 2. 读取配置表
        DataStreamSource<String> tableProcessDwd = env.fromSource(
                FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE_NAME),
                WatermarkStrategy.noWatermarks(),
                "table_process_dwd").setParallelism(1);
        // 2.1注册广播流
        // 2.1.1 转换数据格式
        SingleOutputStreamOperator<TableProcessDwd> processDWDStream = tableProcessDwd.flatMap(
                new FlatMapFunction<String, TableProcessDwd>() {
                    @Override
                    public void flatMap(String s, Collector<TableProcessDwd> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            String op = jsonObject.getString("op");
                            TableProcessDwd processDwd;
                            if ("d".equals(op)) {
                                processDwd = jsonObject.getObject("before", TableProcessDwd.class);
                            } else {
                                processDwd = jsonObject.getObject("after", TableProcessDwd.class);
                            }
                            processDwd.setOp(op);
                            collector.collect(processDwd);
                        } catch (Exception e) {
                            System.out.println("捕获脏数据" + s);
                        }
                    }
                }
        ).setParallelism(1);

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("process_state",
                String.class,
                TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDWDStream.broadcast(
                mapStateDescriptor);

        // 3. 主流连接配置流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = jsonObjStream.connect(broadcastStream).process(
                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    HashMap<String, TableProcessDwd> hashMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection connection = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(
                                connection,
                                "select * from gmall2023_config.table_process_dwd",
                                TableProcessDwd.class,
                                true);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                            hashMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                        }

                    }

                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        String table = jsonObject.getString("table");
                        String type = jsonObject.getString("type");
                        TableProcessDwd processDwd = broadcastState.get(table + ":" + type);
                        if (processDwd == null) {
                            processDwd = hashMap.get(table + ":" + type);
                        }
                        if (processDwd != null) {
                            collector.collect(Tuple2.of(jsonObject, processDwd));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        String op = tableProcessDwd.getOp();
                        String key = tableProcessDwd.getSourceTable() + tableProcessDwd.getSourceType();
                        if ("d".equals(op)) {
                            broadcastState.remove(key);
                            hashMap.remove(key);
                        } else {
                            broadcastState.put(key, tableProcessDwd);
                        }
                    }
                }
        ).setParallelism(1);
//        processStream.print();
        // 4. 去除无关字段
        SingleOutputStreamOperator<JSONObject> dataStream = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObject.getJSONObject("data");
                List<String> list = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !list.contains(key));
                data.put("sink_table",processDwd.getSinkTable());
                return data;
            }
        });



        // 5. 写出到kafka不同主题
        dataStream.sinkTo(FlinkSinkUtil.getKafkaSinkWithTopic());
    }
}

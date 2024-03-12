package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Description
 * @Author lubb
 * @Time 2024-03-12 20:23
 */
public class DwdBaseLog extends BaseAPP {

    public static void main(String[] args) {
        new DwdBaseLog().start(
                10011,
                4,
                "dwd_base_log",
                Constant.TOPIC_LOG
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 业务逻辑编写
        // 1.etl
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 2. 进行新旧访客的修复
        // 注册水位线 按 mid keyby分组
        KeyedStream<JSONObject, String> keyedWithWaterMark = keyedWithWaterMark(jsonObjStream);
        // 修复
        SingleOutputStreamOperator<JSONObject> fixedStream = fixStream(keyedWithWaterMark);

//        fixedStream.print();

        // 3.日志拆分，使用侧输出流实现
        // 启动日志 start err
        // 页面日志 page displays actions err
        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
//        OutputTag<String> pageTag = new OutputTag<>("page", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<>("error", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(fixedStream, startTag, displayTag, actionTag, errorTag);
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);
        SideOutputDataStream<String> errStream = pageStream.getSideOutput(errorTag);


//        pageStream.print("page");
//        startStream.print("start");
//        displayStream.print("display");
//        actionStream.print("action");
//        errStream.print("err");

        // 4. 写出到kafka对应主题
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        errStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

    }

    private static SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> fixedStream, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag, OutputTag<String> errorTag) {
        return fixedStream.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        // 根据数据的不同，输出到不同的侧输出流
                        //
                        JSONObject err = jsonObject.getJSONObject("err");
                        if (err != null) {
                            context.output(errorTag, err.toJSONString());
                            jsonObject.remove("err");
                        }
                        JSONObject page = jsonObject.getJSONObject("page");
                        JSONObject start = jsonObject.getJSONObject("start");
                        JSONObject common = jsonObject.getJSONObject("common");
                        Long ts = jsonObject.getLong("ts");
                        if (start != null) {
                            // 启动日志
                            context.output(startTag, jsonObject.toJSONString());
                        } else if (page != null) {
                            // 页面日志
                            // 获取曝光信息displays
                            JSONArray displays = jsonObject.getJSONArray("displays");
                            if (displays != null) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    display.put("common", common);
                                    display.put("ts", ts);
                                    context.output(displayTag, display.toJSONString());
                                }
                                // 去除曝光信息
                                jsonObject.remove("displays");
                            }
                            // 获取动作信息
                            JSONArray actions = jsonObject.getJSONArray("actions");
                            if (actions != null) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject action = actions.getJSONObject(i);
                                    action.put("common", common);
                                    action.put("ts", ts);
                                    context.output(actionTag, action.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }
                            // 输出页面信息到主流
                            collector.collect(jsonObject.toJSONString());
                        }


                    }
                }
        );
    }


    private static SingleOutputStreamOperator<JSONObject> fixStream(KeyedStream<JSONObject, String> keyedWithWaterMark) {
        return keyedWithWaterMark.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> firstLoginDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 创建状态
                        firstLoginDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("first_login_dt", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector)
                            throws Exception {
                        // 获取当前数据的is_new
                        JSONObject common = jsonObject.getJSONObject("common");
                        String isNew = common.getString("is_new");
                        String firstLoginDt = firstLoginDtState.value();
                        Long ts = jsonObject.getLong("ts");
                        String date = DateFormatUtil.tsToDate(ts);
                        if ("1".equals(isNew)) {
                            // 日志中为新用户
                            if (firstLoginDt != null && firstLoginDt.equals(date)) {
                                // 如果状态不为空 并且状态中记录的与现在不是同一天，则修改为老访客
                                common.put("is_new", "0");
                            } else if (firstLoginDt == null) {
                                // 如果状态为空，则表示没有记录过，更新状态即可
                                firstLoginDtState.update(date);
                            } else {
                                // 与状态中记录的是同一天，不做处理
                            }
                        } else if ("0".equals(isNew)) {
                            // 日志中为老用户
                            if (firstLoginDt == null) {
                                // 状态为空，则该用户是在离线数仓上线之前创建，更新状态为当前的前一天
                                firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                            } else {
                                // 状态存在，不处理
                            }
                        } else {
                            // 数据错误
                        }
                        collector.collect(jsonObject);
                    }
                }
        );
    }

    private static KeyedStream<JSONObject, String> keyedWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                        Duration.ofSeconds(3L)
                ).withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }
                )
        ).keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("common").getString("mid");
                    }
                }
        );
    }

    /**
     * 过滤不完整的数据
     */
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    // 做新老访客修复时keyby该字段会报错
                    JSONObject common = jsonObject.getJSONObject("common");
                    // 注册水位线时会报错
                    Long ts = jsonObject.getLong("ts");

                    if (page != null || start != null) {
                        if (common != null && ts != null) {
                            // 保证修复新老访客字段时不报错
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉脏数据");
                }
            }
        });

    }
}

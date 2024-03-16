package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @description
 * @Author lubb
 * @create 2024-03-16 14:50
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(
                10023,
                4,
                "dws_traffic_home_detail_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心处理
        // 1. 读取dwd中页面访问信息
//        stream.print();
        // 2. 过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3. 按mid分组
        KeyedStream<JSONObject, String> keyedStream = getKeyedStream(jsonObjStream);
        // 4. 判断独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream = processData(keyedStream);
//         processBeanStream.print();

        // 5. 添加水位线
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream = getWithWaterMarkStream(processBeanStream);

        // 6. 分组开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reducedStream = getReduceStream(withWaterMarkStream);
//        reducedStream.print();
        // 7. 写出doris
        SingleOutputStreamOperator<String> jsonStrStream = reducedStream.map(new DorisMapFunction<>());
        jsonStrStream.sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));



    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> getReduceStream(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream) {
        return withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean t1, TrafficHomeDetailPageViewBean t2) throws Exception {
                                t1.setHomeUvCt(t1.getHomeUvCt() + t2.getHomeUvCt());
                                t1.setGoodDetailUvCt(t1.getGoodDetailUvCt() + t2.getGoodDetailUvCt());
                                return t1;
                            }
                        },
                        new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                                String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                                String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                                for (TrafficHomeDetailPageViewBean i : iterable) {
                                    i.setStt(stt);
                                    i.setEdt(edt);
                                    i.setCurDate(curDt);
                                    collector.collect(i);
                                }
                            }
                        });
    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> getWithWaterMarkStream(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream) {
        return processBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, long l) {
                        return trafficHomeDetailPageViewBean.getTs();
                    }
                }));
    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processData(KeyedStream<JSONObject, String> keyedStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream = keyedStream.process(new ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homeLastLoginState;
            ValueState<String> detailLastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取两个状态
                ValueStateDescriptor<String> homeLastLogin = new ValueStateDescriptor<>("home_last_login", String.class);
                homeLastLogin.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                homeLastLoginState = getRuntimeContext().getState(homeLastLogin);

                ValueStateDescriptor<String> detailLastLogin = new ValueStateDescriptor<>("detail_last_login", String.class);
                homeLastLogin.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                detailLastLoginState = getRuntimeContext().getState(homeLastLogin);

            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                JSONObject page = jsonObject.getJSONObject("page");
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                String pageId = page.getString("page_id");
                Long homeUvCt = 0L;
                Long goodDetailUvCt = 0L;
                // 首页独立访客数
                if ("home".equals(pageId)) {
                    String homeLastLoginDt = homeLastLoginState.value();
                    if (homeLastLoginDt == null || homeLastLoginDt.equals(curDt)) {
                        homeUvCt = 1L;
                        // 更新状态
                        homeLastLoginState.update(curDt);
                    }
                } else {
                    // Detail页独立访客数
                    String detailLastLoginDt = detailLastLoginState.value();
                    if (detailLastLoginDt == null || detailLastLoginDt.equals(curDt)) {
                        goodDetailUvCt = 1L;
                        // 更新状态
                        detailLastLoginState.update(curDt);
                    }
                }


                // 如果两个独立访客都为0，可以过滤掉不往下发
                if (homeUvCt != 0 || goodDetailUvCt != 0) {
                    collector.collect(TrafficHomeDetailPageViewBean.builder()
                            .homeUvCt(homeUvCt)
                            .goodDetailUvCt(goodDetailUvCt)
                            .ts(ts)
                            .build());
                }
            }
        });
        return processBeanStream;
    }

    private static KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            JSONObject page = jsonObject.getJSONObject("page");
                            String pageId = page.getString("page_id");
                            String mid = jsonObject.getJSONObject("common").getString("mid");
                            if ("home".equals(pageId) || "good_detail".equals(pageId))
                                if (mid != null)
                                    collector.collect(jsonObject);
                        } catch (Exception e) {
                            System.out.println("过滤数据" + s);
                        }
                    }
                }
        );
    }
}

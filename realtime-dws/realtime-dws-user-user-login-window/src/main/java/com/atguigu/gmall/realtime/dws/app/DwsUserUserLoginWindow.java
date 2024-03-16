package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @description
 * @Author lubb
 * @create 2024-03-16 17:42
 */
public class DwsUserUserLoginWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                10024,
                4,
                "dws_user_user_login_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心逻辑
        // 1. etl
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String uid = jsonObject.getJSONObject("common").getString("uid");
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (uid != null && (lastPageId == null || "login".equals(lastPageId))) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("过滤数据" + s);
                }
            }
        });


        // 3.注册水位线
        SingleOutputStreamOperator<JSONObject> withWaterMark = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));

        // 4. 按uid分组
        KeyedStream<JSONObject, String> keyedStream = withWaterMark.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("uid");
            }
        });

        // 5. 判断独立用户和回流用户
        SingleOutputStreamOperator<UserLoginBean> uuCtBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                state = getRuntimeContext().getState(lastLoginDtDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                String lastLoginDt = state.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                // 回流用户数
                Long backCt = 0L;
                // 独立用户数
                Long uuCt = 0L;
                // 判断独立用户
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    uuCt = 1L;
                }
                // 判断回流用户数
                if (lastLoginDt != null && ts - DateFormatUtil.dateToTs(lastLoginDt) > 7 * 24 * 60 * 60 * 1000L) {
                    backCt = 1L;
                }
                state.update(curDt);

                if (uuCt != 0) {
                    // 不是独立用户一定不是回流用户，无需写到下游
                    collector.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                }
            }
        });
        // 6. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> reduced = uuCtBeanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean t1, UserLoginBean t2) throws Exception {
                                t1.setUuCt(t1.getUuCt() + t2.getUuCt());
                                t1.setBackCt(t1.getBackCt() + t2.getBackCt());
                                return t1;
                            }
                        },
                        new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                                String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                                String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                                for (UserLoginBean userLoginBean : iterable) {
                                    userLoginBean.setStt(stt);
                                    userLoginBean.setEdt(edt);
                                    userLoginBean.setCurDate(curDt);
                                    collector.collect(userLoginBean);
                                }
                            }
                        });
        // 7. 写出doris
        reduced.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_LOGIN_WINDOW));
    }
}

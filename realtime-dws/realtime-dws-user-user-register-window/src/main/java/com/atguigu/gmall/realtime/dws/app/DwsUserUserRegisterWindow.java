package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @description
 * @Author lubb
 * @create 2024-03-16 18:26
 */
public class DwsUserUserRegisterWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(
                10025,
                4,
                "dws_user_user_register_window",
                Constant.TOPIC_DWD_USER_REGISTER
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心处理
        // 1. 读取注册数据
//        stream.print();
        // 2. etl
        // 3.转换为javabean
        SingleOutputStreamOperator<UserRegisterBean> beanStream = stream.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String s, Collector<UserRegisterBean> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String createTime = jsonObject.getString("create_time");
                    String id = jsonObject.getString("id");
                    if (createTime != null && id != null) {
                        collector.collect(new UserRegisterBean("", "", "", 1L, createTime));
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.println("过滤数据" + s);
                }
            }
        });

        // 4. 注册水位线
        SingleOutputStreamOperator<UserRegisterBean> withWaterMark = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(
                new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                        return DateFormatUtil.dateTimeToTs(userRegisterBean.getCreateTime());
                    }
                }
        ));

        // 5. 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> reduced = withWaterMark.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                            @Override
                            public UserRegisterBean reduce(UserRegisterBean u1, UserRegisterBean u2) throws Exception {
                                u1.setRegisterCt(u1.getRegisterCt() + u2.getRegisterCt());
                                return u1;

                            }
                        },
                        new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                                TimeWindow window = context.window();
                                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                                for (UserRegisterBean bean : iterable) {
                                    bean.setStt(stt);
                                    bean.setEdt(edt);
                                    bean.setCurDate(curDt);
                                    collector.collect(bean);
                                }
                            }
                        });

        // 写出doris
        reduced.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_REGISTER_WINDOW));

    }
}

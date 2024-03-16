package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @description
 * @Author lubb
 * @create 2024-03-16 11:32
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务
        // 1. 读取dwd的page日志
        //        stream中已经是读到的page主题的数据
        // 2. 清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3 .按mid分组，判断独立访客
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = keyByMidAndBean(jsonObjStream);
//        beanStream.print();
        // 4.添加水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkStream = getWatermarkStream(beanStream);
        // 5. 分组
         // vc ,ch,ar,is_new
        KeyedStream<TrafficPageViewBean, String> keyedStream = getKeyedStream(withWatermarkStream);

        // 6. 开窗
        WindowedStream<TrafficPageViewBean, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // 7. 聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduced = getReduceStream(windowedStream);
//        reduced.print();
        // 8. 写出doris
        reduced.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));
    }

    private static SingleOutputStreamOperator<TrafficPageViewBean> getReduceStream(WindowedStream<TrafficPageViewBean, String, TimeWindow> windowedStream) {
        return windowedStream.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        // 聚合
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setSvCt(value1.getSvCt() + value1.getSvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setDurSum(value1.getDurSum() + value1.getDurSum());
                        return value1;
                    }
                }
                ,
                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TrafficPageViewBean bean : iterable) {
                            bean.setStt(stt);
                            bean.setEdt(edt);
                            bean.setCur_date(curDt);
                            collector.collect(bean);
                        }
                    }
                });
    }

    private static KeyedStream<TrafficPageViewBean, String> getKeyedStream(SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkStream) {
        return withWatermarkStream.keyBy(new KeySelector<TrafficPageViewBean, String>() {
            @Override
            public String getKey(TrafficPageViewBean value) throws Exception {
                return value.getVc() + ":" +
                        value.getCh() + ":" +
                        value.getAr() + ":" +
                        value.getIsNew();
            }
        });
    }

    private static SingleOutputStreamOperator<TrafficPageViewBean> getWatermarkStream(SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
        return beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(
                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                        return trafficPageViewBean.getTs();
                    }
                }
        ));
    }

    private static SingleOutputStreamOperator<TrafficPageViewBean> keyByMidAndBean(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = jsonObjStream.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("common").getString("mid");
                    }
                }
        ).process(
                new ProcessFunction<JSONObject, TrafficPageViewBean>() {
                    ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 创建状态
                        ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                        // 设置状态存活时间
                        lastLoginDtDesc.enableTimeToLive(StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());
                        state = getRuntimeContext().getState(lastLoginDtDesc);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                        JSONObject page = jsonObject.getJSONObject("page");
                        JSONObject common = jsonObject.getJSONObject("common");
                        // 判断独立访客
                        Long ts = jsonObject.getLong("ts");
                        String curDt = DateFormatUtil.tsToDate(ts);
                        String lastLoginDt = state.value();
                        Long uvCt = 0L;
                        Long svCt = 0L;
                        // 判断独立访客
                        if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                            // 是独立访客
                            uvCt = 1L;
                            // 更新状态
                            state.update(curDt);
                        }
                        // 判断会话数
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null) {
                            svCt = 1L;
                        }

                        collector.collect(TrafficPageViewBean.builder()
                                .uvCt(uvCt)
                                .svCt(svCt)
                                .pvCt(1L)
                                .durSum(page.getLong("during_time"))
                                .vc(common.getString("vc"))
                                .ar(common.getString("ar"))
                                .ch(common.getString("ch"))
                                .isNew(common.getString("is_new"))
                                .ts(ts)
                                .sid(common.getString("sid"))
                                .build());
                    }
                }
        );
        return beanStream;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            Long ts = jsonObject.getLong("ts");
                            String mid = jsonObject.getJSONObject("common").getString("mid");
                            if (mid != null && ts != null) {
                                // 对分组字段判空，防止出现数据传不到下个算子的报错
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("过滤掉脏数据" + s);
                        }
                    }
                }
        );
    }
}

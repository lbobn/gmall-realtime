package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @description
 * @Author lubb
 * @create 2024-03-19 14:32
 */
public class DwsTradeProvinceOrderWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10030,
                1,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心处理
        // 1. 读取数据
//        stream.print();
        // 2. 过滤
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);
        
        // 3.添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMark = getWithWaterMark(jsonObjStream);
        
        // 4. 度量值去重 ，转换数据结构
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanStream = processAndTransform(withWaterMark);

        // 5. 分组开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedStream = getReduce(beanStream);
//        reducedStream.print();

        // 6. 补全维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> provinceNameStream = joinDimInfo(reducedStream);

//        provinceNameStream.print();
        provinceNameStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }

    private static SingleOutputStreamOperator<TradeProvinceOrderBean> joinDimInfo(SingleOutputStreamOperator<TradeProvinceOrderBean> reducedStream) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> provinceNameStream = AsyncDataStream.unorderedWait(reducedStream, new DimAsyncFunction<TradeProvinceOrderBean>() {
            @Override
            public String getRowKey(TradeProvinceOrderBean bean) {
                return bean.getProvinceId();
            }

            @Override
            public String getTableName() {
                return "dim_base_province";
            }

            @Override
            public void join(TradeProvinceOrderBean bean, JSONObject dimInfo) {
                bean.setProvinceName(dimInfo.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);
        return provinceNameStream;
    }

    private static SingleOutputStreamOperator<TradeProvinceOrderBean> getReduce(SingleOutputStreamOperator<TradeProvinceOrderBean> beanStream) {
        return beanStream.keyBy(new KeySelector<TradeProvinceOrderBean, String>() {
                    @Override
                    public String getKey(TradeProvinceOrderBean tradeProvinceOrderBean) throws Exception {
                        return tradeProvinceOrderBean.getProvinceId();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean t1, TradeProvinceOrderBean t2) throws Exception {
                                t1.setOrderAmount(t1.getOrderAmount().add(t2.getOrderAmount()));
                                t1.getOrderIdSet().addAll(t2.getOrderIdSet());
                                return t1;

                            }
                        },
                        new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                                TimeWindow window = context.window();
                                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());

                                for (TradeProvinceOrderBean bean : iterable) {
                                    bean.setEdt(edt);
                                    bean.setStt(stt);
                                    bean.setCurDate(curDt);
                                    bean.setOrderCount((long) bean.getOrderIdSet().size());
                                    collector.collect(bean);
                                }

                            }
                        });
    }

    private static SingleOutputStreamOperator<TradeProvinceOrderBean> processAndTransform(SingleOutputStreamOperator<JSONObject> withWaterMark) {
        KeyedStream<JSONObject, String> keyedStream = withWaterMark.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("order_id");
            }
        });

        SingleOutputStreamOperator<TradeProvinceOrderBean> beanStream = keyedStream.map(new RichMapFunction<JSONObject, TradeProvinceOrderBean>() {
            ValueState<BigDecimal> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BigDecimal> lastTotalAmountDesc = new ValueStateDescriptor<>("last_total_amount", BigDecimal.class);
                lastTotalAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30)).build());
                state = getRuntimeContext().getState(lastTotalAmountDesc);
            }

            @Override
            public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
                HashSet<String> hashSet = new HashSet<>();
                hashSet.add(jsonObject.getString("order_id"));
                BigDecimal lastTotalAmount = state.value();
                lastTotalAmount = lastTotalAmount == null ? new BigDecimal("0") : lastTotalAmount;
                BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");

                return TradeProvinceOrderBean.builder()
                        .orderIdSet(hashSet)
                        .provinceId(jsonObject.getString("province_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .orderAmount(splitTotalAmount.subtract(lastTotalAmount))
                        .build();
            }
        });
        return beanStream;
    }

    private static SingleOutputStreamOperator<JSONObject> getWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(
                new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }
        ));
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String id = jsonObject.getString("id");
                    String orderId = jsonObject.getString("order_id");
                    String provinceId = jsonObject.getString("province_id");
                    Long ts = jsonObject.getLong("ts");
                    if (id != null && orderId != null && provinceId != null && ts != null) {
                        jsonObject.put("ts", ts * 1000);
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("过滤数据" + s);
                }
            }
        });
    }
}

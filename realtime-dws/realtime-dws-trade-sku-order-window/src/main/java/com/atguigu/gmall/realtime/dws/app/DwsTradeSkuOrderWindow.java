package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @description 传统的维度关联
 * @Author lubb
 * @create 2024-03-18 16:53
 */
public class DwsTradeSkuOrderWindow extends BaseAPP {
    /*public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(
                10027,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }*/

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务

        // 1. 读取Dwd order detail
//        stream.print();
        // 2. 清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3. 添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMark = getWithWaterMark(jsonObjStream);

        // 4. 修正度量值 装换数据结构
        // 按id分组
        KeyedStream<JSONObject, String> keyedStream = getKeyBy(withWaterMark);
        SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream = getProcessBeanStream(keyedStream);
//        processBeanStream.print();

        // 5. 分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream = getReduceBeanStream(processBeanStream);
//        reduceBeanStream.print();
        // 6. 关联维度
        SingleOutputStreamOperator<TradeSkuOrderBean> fullDimBeanStream = getFullDimBeanStream(reduceBeanStream);

        // 7. 写出doris
        fullDimBeanStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW));
    }

    private static SingleOutputStreamOperator<TradeSkuOrderBean> getFullDimBeanStream(SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> fullDimBeanStream = reduceBeanStream.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = HBaseUtil.getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeConnection(connection);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                        // 1.读取HBase维度表信息
                        JSONObject dimSkuInfo = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_sku_info", bean.getSkuId());
                        // 补全
                        bean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                        bean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                        bean.setSpuId(dimSkuInfo.getString("spu_id"));
                        bean.setSkuName(dimSkuInfo.getString("sku_name"));
                        // 2.
                        JSONObject dimSpuInfo = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_spu_info", bean.getSkuId());
                        bean.setSpuName(dimSpuInfo.getString("spu_name"));
                        // 3.
                        JSONObject dimBaseCategory3 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category3", bean.getSkuId());
                        bean.setCategory3Name(dimBaseCategory3.getString("name"));
                        bean.setCategory2Id(dimBaseCategory3.getString("category2_id"));
                        // 4.
                        JSONObject dimBaseCategory2 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category2", bean.getSkuId());
                        bean.setCategory2Name(dimBaseCategory2.getString("name"));
                        bean.setCategory1Id(dimBaseCategory2.getString("category1_id"));
                        // 5.
                        JSONObject dimBaseCategory1 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category1", bean.getSkuId());
                        bean.setCategory1Name(dimBaseCategory1.getString("name"));
                        // 6.
                        JSONObject dimBaseTrademark = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_trademark", bean.getSkuId());
                        bean.setTrademarkName(dimBaseTrademark.getString("tm_name"));

                        return bean;
                    }
                }
        );
        return fullDimBeanStream;
    }

    private static SingleOutputStreamOperator<TradeSkuOrderBean> getReduceBeanStream(SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream = processBeanStream
                .keyBy(new KeySelector<TradeSkuOrderBean, String>() {
                    @Override
                    public String getKey(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                        return tradeSkuOrderBean.getSkuId();
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean t1, TradeSkuOrderBean t2) throws Exception {
                                t1.setOriginalAmount(t1.getOriginalAmount().add(t2.getOriginalAmount()));
                                t1.setOrderAmount(t1.getOrderAmount().add(t2.getOrderAmount()));
                                t1.setActivityReduceAmount(t1.getActivityReduceAmount().add(t2.getActivityReduceAmount()));
                                t1.setCouponReduceAmount(t1.getCouponReduceAmount().add(t2.getCouponReduceAmount()));
                                return t1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                                TimeWindow window = context.window();
                                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                                for (TradeSkuOrderBean i : iterable) {
                                    i.setStt(stt);
                                    i.setEdt(edt);
                                    i.setCurDate(curDt);
                                    collector.collect(i);
                                }
                            }
                        });
        return reduceBeanStream;
    }

    private static SingleOutputStreamOperator<TradeSkuOrderBean> getProcessBeanStream(KeyedStream<JSONObject, String> keyedStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            MapState<String, BigDecimal> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmountDesc = new MapStateDescriptor<>("last_amount", String.class, BigDecimal.class);
                lastAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                state = getRuntimeContext().getMapState(lastAmountDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                // 相同id 的数据，后面的减去状态里的数据 相当于后面数据都是与上条数据的差值
                /* 如
                    1     .     10 20 0  0     ->下游 10,20,0,0  -->  state(10,20, 0, 0)
                *   2(重复).     10 20 30 40    ->下游 (0,0,0,0)  -->   state (10,20,30,40)
                    后续在sum等操作就不会出错
                    聚合  ：     10 20 30  40
                * */
                BigDecimal originalAmount = state.get("originalAmount");
                BigDecimal activityReduceAmount = state.get("activityReduceAmount");
                BigDecimal couponReduceAmount = state.get("couponReduceAmount");
                BigDecimal orderAmount = state.get("orderAmount");
//                if (orderAmount != null) {
//                    System.out.println("相同id" + jsonObject);
//                }
                // 非0判断
                orderAmount = orderAmount == null ? new BigDecimal("0") : orderAmount;
                activityReduceAmount = activityReduceAmount == null ? new BigDecimal("0") : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? new BigDecimal("0") : couponReduceAmount;
                originalAmount = originalAmount == null ? new BigDecimal("0") : originalAmount;

                BigDecimal curOriginalAmount = jsonObject.getBigDecimal("order_price").multiply(jsonObject.getBigDecimal("sku_num"));


                TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
                        .skuId(jsonObject.getString("sku_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .originalAmount(curOriginalAmount.subtract(originalAmount))
                        .orderAmount(jsonObject.getBigDecimal("split_total_amount").subtract(orderAmount))
                        .activityReduceAmount(jsonObject.getBigDecimal("split_activity_amount").subtract(activityReduceAmount))
                        .couponReduceAmount(jsonObject.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount))
                        .build();

                // 更新状态
                state.put("originalAmount", curOriginalAmount);
                state.put("orderAmount", jsonObject.getBigDecimal("split_total_amount"));
                state.put("activityReduceAmount", jsonObject.getBigDecimal("split_activity_amount"));
                state.put("couponReduceAmount", jsonObject.getBigDecimal("split_coupon_amount"));
                collector.collect(bean);
            }
        });
        return processBeanStream;
    }

    private static KeyedStream<JSONObject, String> getKeyBy(SingleOutputStreamOperator<JSONObject> withWaterMark) {
        return withWaterMark.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("id");
            }
        });
    }

    private static SingleOutputStreamOperator<JSONObject> getWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    if (s != null) {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        Long ts = jsonObject.getLong("ts");
                        String id = jsonObject.getString("id");
                        String skuId = jsonObject.getString("sku_id");
                        if (ts != null && id != null && skuId != null) {
                            jsonObject.put("ts", ts * 1000);
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("过滤数据" + s);
                }
            }
        });
    }
}

package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.app.functions.DimBroadcastProcessFunction;
import com.atguigu.gmall.realtime.dim.app.functions.DimHbaseRichFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 * Dim维度层
 * */
public class DimAPP extends BaseAPP {
    public static void main(String[] args) {
        new DimAPP().start(
                11001,
                4,
                "dim_app",
                Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑
        // 1. 数据清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 2. 使用flinkCDC监控配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(
                Constant.PROCESS_DATABASE,
                Constant.PROCESS_DIM_TABLE_NAME);
        //读取数据
        DataStreamSource<String> mysqlSource = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "mysql_source").setParallelism(1); // 设置并行度为1 才能读到后续cud的数据
//        mysqlSource.print();

        // 3. 在HBase中创建表
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHBaseTable(mysqlSource);
//        createTableStream.print();

        // 4. 创建广播流
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>(
                "broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = createTableStream.broadcast(broadcastState);

        // 5. 连接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream =
                connect(jsonObjStream, broadcastState, broadcastStream);
//        dimStream.print();
//         6. 筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dim = filterCols(dimStream);
//        dim.print();


        // 7. 写出到HBase
        dim.addSink(new DimHbaseRichFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterCols(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        return dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value)
                    throws Exception {
                JSONObject jsonObject = value.f0;
                List<String> columns = new ArrayList<>(Arrays.asList(
                        value.f1.getSinkColumns().split(",")));
//                columns.add("op_type");
//                System.out.println(columns);
                JSONObject data = jsonObject.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return value;
            }
        });
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(
            SingleOutputStreamOperator<JSONObject> jsonObjStream,
            MapStateDescriptor<String, TableProcessDim> broadcastState,
            BroadcastStream<TableProcessDim> broadcastStream) {
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = jsonObjStream.connect(
                broadcastStream);

        return connectedStream.process(new DimBroadcastProcessFunction(broadcastState)).setParallelism(1);
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(
            DataStreamSource<String> mysqlSource) {
        SingleOutputStreamOperator<TableProcessDim> createTableStream = mysqlSource.flatMap(
                new RichFlatMapFunction<String, TableProcessDim>() {

                    Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        connection = HBaseUtil.getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        HBaseUtil.closeConnection(connection);
                    }

                    @Override
                    public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            String op = jsonObject.getString("op");
                            TableProcessDim dim;
                            if ("d".equals(op)) {
                                dim = jsonObject.getObject("before", TableProcessDim.class);
                                //删除表格
                                deleteTable(dim);
                            } else if ("c".equals(op) || "r".equals(op)) {
                                dim = jsonObject.getObject("after", TableProcessDim.class);
                                // 修改表格
                                createTable(dim);
                            } else {
                                dim = jsonObject.getObject("after", TableProcessDim.class);
                                deleteTable(dim);
                                createTable(dim);
                            }
                            dim.setOp(op);
                            collector.collect(dim);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    private void createTable(TableProcessDim dim) {
                        String sinkFamily = dim.getSinkFamily();
                        String[] split = sinkFamily.split(",");
                        try {
                            HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    private void deleteTable(TableProcessDim dim) {
                        try {
                            HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        return createTableStream;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) &&
                            !"bootstarp-start".equals(type) &&
                            !"bootstrap-complete".equals(type)
                            && data != null && data.size() != 0) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}

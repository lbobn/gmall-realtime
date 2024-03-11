package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class DimAPP extends BaseAPP {
    public static void main(String[] args) {
        new DimAPP().start(11001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑
        // 1. 数据清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 2. 使用flinkCDC监控配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        //读取数据
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
//        mysqlSource.print();

        // 3. 在HBase中创建表
        SingleOutputStreamOperator<TableProcessDim> createTableStream = mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {

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
        createTableStream.print();
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

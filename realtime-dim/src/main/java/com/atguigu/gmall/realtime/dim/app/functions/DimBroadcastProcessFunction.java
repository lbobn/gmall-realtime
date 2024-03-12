package com.atguigu.gmall.realtime.dim.app.functions;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject,
        TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    public Map<String, TableProcessDim> hashMap;
    MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastProcessFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 连接Mysql 在主流之前读取一次mysql维度表配置
        java.sql.Connection connection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(connection, "select * from gmall2023_config.table_process_dim", TableProcessDim.class, true);
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeConnection(connection);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        String key = jsonObject.getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = readOnlyContext.getBroadcastState(broadcastState);
        TableProcessDim tableProcessDim = tableProcessState.get(key);
        if (tableProcessDim == null) {
            // 广播流中没有查到，到本地的hashmap中查
            tableProcessDim = hashMap.get(key);
        }
        if (tableProcessDim != null) {
            // 查到相应数据
            collector.collect(Tuple2.of(jsonObject, tableProcessDim));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {

        BroadcastState<String, TableProcessDim> tableProcessState = context.getBroadcastState(broadcastState);
        String op = tableProcessDim.getOp();
        String key = tableProcessDim.getSourceTable();
        if ("d".equals(op)) {
            tableProcessState.remove(key);
            // 本地map也要删除
            hashMap.remove(key);
        } else {
            tableProcessState.put(key, tableProcessDim);
        }
    }
}

package com.atguigu.gmall.realtime.dim.app.functions;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class DimHbaseRichFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    Connection connection;
    Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = HBaseUtil.getConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        super.close();
        HBaseUtil.closeConnection(connection);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObject = value.f0;
        TableProcessDim f1 = value.f1;
        String type = jsonObject.getString("type");
        JSONObject data = jsonObject.getJSONObject("data");
        if("delete".equals(type)){
            // 删除
            deleteHBaseCell(data,f1);
        }else{
            // 覆盖写入
            writeToHbase(data,f1);
        }

        // 判断redis是否需要变化
        if("delete".equals(type) || "update".equals(type)){
            // 删除redis中对应的key-value
            String key = RedisUtil.getKey(f1.getSinkTable(), data.getString(f1.getSinkRowKey()));
            jedis.del(key);
        }

    }

    private void writeToHbase(JSONObject data, TableProcessDim f1) {
        String sinkTable = f1.getSinkTable();
        String key = data.getString(f1.getSinkRowKey());
        String sinkFamily = f1.getSinkFamily();
        try {
            HBaseUtil.putCells(connection, Constant.HBASE_NAMESPACE, sinkTable, key, sinkFamily, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void deleteHBaseCell(JSONObject data, TableProcessDim f1) {
        String sinkTable = f1.getSinkTable();
        String row = data.getString(f1.getSinkRowKey());
        try {
            HBaseUtil.deleteCells(connection,Constant.HBASE_NAMESPACE, sinkTable, row);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

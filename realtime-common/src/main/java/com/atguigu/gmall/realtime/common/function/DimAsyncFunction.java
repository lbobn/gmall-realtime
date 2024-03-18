package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @description
 * @Author lubb
 * @create 2024-03-18 22:44
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;



    @Override
    public void open(Configuration parameters) throws Exception {

        // 获取异步连接
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConnection);
    }

    @Override
    public void asyncInvoke(T bean, ResultFuture<T> resultFuture) throws Exception {
        String rowKey = getRowKey(bean);
        String tableName = getTableName();
        String key = RedisUtil.getKey(tableName, rowKey);

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                // 第一步异步访问的值
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(key);
                String dim_info = null;
                try {
                    dim_info= dimSkuInfoFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                return dim_info;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String s) {
                JSONObject jsonObject = null;
                if(s == null || s.length() == 0){
                    //访问hbase
                    try {
                        jsonObject = HBaseUtil.getAsyncCells(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
                        // 将读取的数据保存到redis
                        redisAsyncConnection.async().setex(key,24*60*60,jsonObject.toJSONString());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }else{
                    // redis 中存在数据
                    jsonObject = JSONObject.parseObject(s);
                }
                return jsonObject;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject jsonObject) {
                // 合并维度并返回
                if(jsonObject == null){
                    // 无维度数据
                }else{
                    join(bean,jsonObject);
                }
                resultFuture.complete(Collections.singletonList(bean));
            }
        });
    }


}

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
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        String rowKey = getRowKey(input);
        String tableName = getTableName();
        String key = RedisUtil.getKey(tableName, rowKey);

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
//                System.out.println("获取到的redis连接=========>" + redisAsyncConnection);
                // 第一步异步访问的值
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(key);
                String dimInfo = null;
                try {
                    dimInfo= dimSkuInfoFuture.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return dimInfo;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject dimJsonObj = JSONObject.parseObject(dimInfo);
                if(dimJsonObj == null || dimJsonObj.size() == 0){
                    //访问hbase
                    try {
                        dimJsonObj = HBaseUtil.getAsyncCells(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
//                        System.out.println("读取到Hbase数据===>" + dimJsonObj.toJSONString());
                        // 将读取的数据保存到redis
                        redisAsyncConnection.async().setex(key,24*60*60,dimJsonObj.toJSONString());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }else{
                    // redis 中存在数据
//                    dimJsonObj = JSONObject.parseObject(dimInfo);
//                    System.out.println("redis中读取到数据===>"+dimJsonObj.toJSONString());
                }
                return dimJsonObj;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dim) {
                // 合并维度并返回
                if(dim == null){
                    // 无维度数据
                    System.out.println("无维度信息");
                }else{
                    join(input,dim);
                }
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }


}

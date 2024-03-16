package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @description
 * @Author lubb
 * @create 2024-03-16 14:33
 */
public class DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T t) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSONObject.toJSONString(t, config);
    }
}

package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;

/**
 * @description
 * @Author lubb
 * @create 2024-03-18 23:00
 */
public interface DimJoinFunction<T> {
    public abstract String getRowKey(T bean);
    public abstract String getTableName();

    public abstract void join(T bean, JSONObject jsonObject);
}

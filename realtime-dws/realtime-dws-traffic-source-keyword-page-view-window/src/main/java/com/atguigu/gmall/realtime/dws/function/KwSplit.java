package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.common.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @description 自定义UDTF函数
 * @Author lubb
 * @create 2024-03-15 21:27
 */
@FunctionHint(output = @DataTypeHint("row<keyword STRING>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String s) {
        if (s == null) {
            return;
        }
        List<String> split = IkUtil.Split(s);
        for (String s1 : split) {
            collect(Row.of(s1));
        }
    }

}

package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description
 * @Author lubb
 * @create 2024-03-15 21:30
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        // 处理逻辑
        // 1. 读取dwd中页面浏览日志
        getDwdPageTable(tableEnv);
        // 2. 取出搜索关键字
//        tableEnv.executeSql("select * from page_log").print();
        filterSearchKeyword(tableEnv);
        // 3. 注册自定义的分词函数，使用该函数分词
        tableEnv.createTemporaryFunction("kw_split", KwSplit.class);
        splitKeyword(tableEnv);

        // 4. 开窗聚合和tvf
        Table result = getAggratedTable(tableEnv);


        // 5. 写出到doris
        // 建立doris映射
        createDorisSinkMapping(tableEnv);
        //写出
        result.insertInto("dws_traffic_source_keyword_page_view_window").execute();

    }

    private static void createDorisSinkMapping(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")"+SQLUtil.getDorisSink("dws_traffic_source_keyword_page_view_window"));
    }

    private static Table getAggratedTable(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery(
                "select cast(TUMBLE_START(row_time, INTERVAL '10' SECOND) as STRING) as stt,\n" +
                "       cast(TUMBLE_END(row_time, INTERVAL '10' SECOND) as STRING)  as edt,\n" +
                "       cast(CURRENT_DATE as STRING)   cur_date,\n" +
                "       keyword,\n" +
                "       count(*)                                        keyword_count\n" +
                "from keyword_table\n" +
                "GROUP BY TUMBLE(row_time, INTERVAL '10' SECOND),\n" +
                "         keyword");
        return table;
    }

    private static void splitKeyword(StreamTableEnvironment tableEnv) {
        Table keywordTable = tableEnv.sqlQuery("select\n" +
                "kw,\n"+
                "    keyword,\n" +
                "    row_time\n" +
                "from kws_table\n" +
                "join lateral table ( kw_split(kw) ) on true");
        tableEnv.createTemporaryView("keyword_table", keywordTable);
    }

    private static void filterSearchKeyword(StreamTableEnvironment tableEnv) {
        Table kwsTable = tableEnv.sqlQuery("select\n" +
                "    page['item'] kw,\n" +
                "    row_time\n" +
                "from page_log\n" +
                "where (page['last_page_id'] = 'search'\n" +
                "or page['last_page_id'] = 'home')\n" +
                "and page['item_type'] = 'keyword'\n" +
                "and page['item'] is not null ");
        tableEnv.createTemporaryView("kws_table", kwsTable);
    }

    private static TableResult getDwdPageTable(StreamTableEnvironment tableEnv) {
        return tableEnv.executeSql("create table page_log(\n" +
                "    page map<string,string>,\n" +
                "    ts bigint,\n" +
                "    row_time as to_timestamp_ltz(ts,3),\n" +
                "    watermark for row_time as row_time - interval '5' second\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE,
                "dws_traffic_source_keyword_page_view_window"));
    }
}

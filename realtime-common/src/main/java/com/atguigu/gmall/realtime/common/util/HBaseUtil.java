package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class HBaseUtil {
    /**
     * 获取HBase连接
     *
     * @return 同步连接对象
     */
    public static Connection getConnection() {
        Connection connection = null;


        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    /**
     * 关闭连接
     *
     * @param connection 同步连接对象
     */
    public static void closeConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建HBase表格
     *
     * @param connection 同步连接对象
     * @param namespace  命名空间
     * @param tableName  表名
     * @param family     列族
     * @throws IOException 获取Admin连接异常
     */
    public static void createTable(Connection connection, String namespace, String tableName, String... family)
            throws IOException {
        if (family == null || family.length == 0) {
            System.out.println("HBase 建表列族至少为1");
            return;
        }
        Admin admin = connection.getAdmin();

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(namespace, tableName));

        for (String s : family) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(s)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }

        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
//            throw new RuntimeException(e);
            System.out.println("表格" + namespace + ":" + tableName + "已创建，无需重复创建");
        }

        admin.close();
    }

    /**
     * 删除表格
     *
     * @param connection 同步连接对象
     * @param namespace  命名空间
     * @param tableName  表名
     * @throws IOException 获取Admin连接异常
     */
    public static void dropTable(Connection connection, String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();

        try {
            admin.disableTable(TableName.valueOf(namespace, tableName));
            admin.deleteTable(TableName.valueOf(namespace, tableName));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        admin.close();
    }

    /**
     * 写出数据到HBase
     * @param connection 同步连接
     * @param namespace 命名空间
     * @param tableName 表名
     * @param rowKey 行键
     * @param family 列族
     * @param data 数据
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        // 获取Table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        // 数据写出
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String col : data.keySet()) {
            String colValue = data.getString(col);
            if(colValue != null){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(col),Bytes.toBytes(colValue));
            }
        }
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //关闭Table
        table.close();
    }

    public static JSONObject getCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        JSONObject jsonObject = new JSONObject();
        Get get = new Get(Bytes.toBytes(rowKey));


        try {
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                jsonObject.put(new String(CellUtil.cloneQualifier(cell)),new String(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        table.close();
        return jsonObject;
    }

    public static JSONObject getAsyncCells(AsyncConnection asyncConnection, String namespace, String tableName, String rowKey) throws IOException {
        AsyncTable<AdvancedScanResultConsumer> table = asyncConnection.getTable(TableName.valueOf(namespace,tableName));

        JSONObject jsonObject = new JSONObject();
        Get get = new Get(Bytes.toBytes(rowKey));


        try {
            Result result = table.get(get).get();
            for (Cell cell : result.rawCells()) {
                jsonObject.put(new String(CellUtil.cloneQualifier(cell)),new String(CellUtil.cloneValue(cell)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return jsonObject;
    }

    /**
     * 删除整行数据
     * @param connection 同步连接
     * @param namespace 命名空间
     * @param tableName 表名
     * @param rowKey 行键
     */
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        // 获取Table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        //删除
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //关闭Table
        table.close();
    }

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

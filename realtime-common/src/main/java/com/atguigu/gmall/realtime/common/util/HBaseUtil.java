package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 *
 */
public class HBaseUtil {
    /**
     * 获取HBase连接
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
     * @param connection 同步连接对象
     * @param namespace 命名空间
     * @param tableName 表名
     * @param family 列族
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
                .newBuilder(TableName.valueOf(namespace,tableName));

        for (String s : family) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(s)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }

        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        admin.close();
    }

    /**
     * 删除表格
     * @param connection 同步连接对象
     * @param namespace 命名空间
     * @param tableName 表名
     * @throws IOException 获取Admin连接异常
     */
    public static void dropTable(Connection connection, String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();

        try {
            admin.disableTable(TableName.valueOf(namespace,tableName));
            admin.deleteTable(TableName.valueOf(namespace,tableName));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        admin.close();
    }

}

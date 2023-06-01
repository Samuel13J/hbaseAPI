package org.leelen.hbase;

/**
 * @author wangjie
 * @create 2023-05-10 11:56
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseAPI {

    private static Configuration conf;
    private static Connection connection = null;
    private static String tableName = "leelen_iot:home_linkage_wide";



    static {
//        新建一个Configuration
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "ld-bp19x2n530lt779sd-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30020");
        conf.set("hbase.client.username", "admin");
        conf.set("hbase.client.password", "admin");
        conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
        ExecutorService threads = Executors.newFixedThreadPool(20);
        try {
            connection = ConnectionFactory.createConnection(conf, threads);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Result getData(String tableName, String rowKey, Integer startRow, Integer endRow) throws IOException {

        //Table为非线程安全对象，每个线程在对Table操作时，都必须从Connection中获取相应的Table对象
        Result res = null;
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
//            //单行读取
            Get get = new Get(Bytes.toBytes(rowKey));
            res = table.get(get);
        }
        return res;
    }

    /**
     * 批量查询hbase数据   1w rowkey 大概需要 2s - 3s才能查询出来  1k rowkey 大概需要 507 ms
     * @param rowkeyList
     * @return
     * @throws IOException
     */
    public static List<String> qurryTableTestBatch(Table table, List<String> rowkeyList) throws IOException {
        List<Get> getList = new ArrayList();
        //把rowkey加到get里，再把get装到list中
        for (String rowkey : rowkeyList){
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        List<String> list = new ArrayList<>();
        //重点在这，直接查getList<Get>
        Result[] results = table.get(getList);
        //对返回的结果集进行操作
        for (Result result : results){
            for (Cell kv : result.rawCells()) {
                String value = Bytes.toString(CellUtil.cloneValue(kv));
                list.add(value);
            }
        }
        return list;
    }

    /**
     * 批量查询 hbase数据，1w rowkey 大概需要 2s - 3s才能查询出来  1k rowkey 大概需要 980 ms  方法其实同上
     * @param table
     * @param rows
     * @return
     * @throws Exception
     */
    public static List<String> getData(Table table, List<String> rows) throws Exception {
        List<Get> gets = new ArrayList<>();
        for (String str : rows) {
            Get get = new Get(Bytes.toBytes(str));
            gets.add(get);
        }
        List<String> values = new ArrayList<>();
        Result[] results = table.get(gets);

        for (Result result : results) {
            for (Cell kv : result.rawCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(kv));
                String qualifire = Bytes.toString(CellUtil.cloneQualifier(kv));
                String value = Bytes.toString(CellUtil.cloneValue(kv));
                values.add(value);
            }
        }
        return values;
    }

    public static void main(String[] args) throws Exception {
        List<String> rowKeyList = new ArrayList<>();
        // 获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (long i = 1661698512451588206L; i < 1661698512451598206L; i++) {
            rowKeyList.add(String.valueOf(i));
        }
        long startTime = System.currentTimeMillis();
        List<String> strings = qurryTableTestBatch(table, rowKeyList);
//        List<String>  strings = getData(table, rowKeyList);
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间 :  " + (endTime - startTime) + " ms");
    }

}

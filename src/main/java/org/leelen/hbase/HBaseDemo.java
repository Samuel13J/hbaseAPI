package org.leelen.hbase;

/**
 * @author wangjie
 * @create 2023-05-24 14:37
 */

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        conf = HBaseConfiguration.create();
//        集群的连接地址(VPC内网地址)在控制台页面的数据库连接界面获得
        conf.set("hbase.zookeeper.quorum", "ld-bp19x2n530lt779sd-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30020");
        conf.set("hbase.client.username", "admin");
        conf.set("hbase.client.password", "admin");
//        如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());

        //Connection为线程安全对象，在整个程序的生命周期里，只需构造一个Connection对象
        //在程序结束后，需要将Connection对象关闭，否则会造成连接泄露。可以采用try finally方式防止泄露
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            Admin admin = connection.getAdmin();

            //在HBase中创建表
            createTable(admin, "tablename", Bytes.toBytes("familyname"));

            //Table 为非线程安全对象，每个线程在对Table操作时，都必须从Connection中获取相应的Table对象
            Table table = connection.getTable(TableName.valueOf("tablename"));

            //在HBase中插入一行数据，如果需要插入多行，可以调用table.put(List<Put> puts)接口
//            putData(table, Bytes.toBytes("row"), Bytes.toBytes("family"), Bytes.toBytes("column1"),
//                    Bytes.toBytes("value1"), Bytes.toBytes("column2"), Bytes.toBytes("value2"));

            //在HBase中删除一行数据
            deleteData(table, Bytes.toBytes("row"));

            //在HBase中获取单行数据
            Result result = getData(table, Bytes.toBytes("row"));

            //在HBase中范围扫描数据
            scanData(table, Bytes.toBytes("startKey"), Bytes.toBytes("endKey"), 10);

            //Disable Table
            disableTable(admin, "tablename");

            //删除Table
            deleteTable(admin, "tablename");

            //使用完Table和Admin后，close
            table.close();
            admin.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * 插入/更新一行的两个列
     * @param row rowKey
     * @param family family名字
     * @param column1 列1的名字
     * @param value1 列1的值
     * @param column2 列2的名字
     * @param value2 列2的值
     * @throws IOException
     */
//    public static void putData(Table table, byte[] row, byte[] family,
//                               byte[] column1, byte[] value1, byte[] column2, byte[] value2) throws IOException {
//        Put put = new Put(row);
//        put.addColumn(family, column1, value1);
//        put.addColumn(family, column2, value2);
//        table.put(put);
//    }

    public static void putData(Table table, byte[] row, byte[] family, byte[] column1, byte[] value1, byte[] column2, byte[] value2,
                               byte[] column3, byte[] value3, byte[] column4, byte[] value4, byte[] column5, byte[] value5,
                               byte[] column6, byte[] value6, byte[] column7, byte[] value7) throws IOException {
        Put put = new Put(row);
        put.addColumn(family, column1, value1);
        put.addColumn(family, column2, value2);
        put.addColumn(family, column3, value3);
        put.addColumn(family, column4, value4);
        put.addColumn(family, column5, value5);
        put.addColumn(family, column6, value6);
        put.addColumn(family, column7, value7);
        table.put(put);
    }

    public static void putData1(Table table, byte[] row, byte[] family1, byte[] column1, byte[] value1, byte[] family2, byte[] column2, byte[] value2,
            byte[] family3, byte[] column3, byte[] value3, byte[] family4, byte[] column4, byte[] value4, byte[] family5, byte[] column5, byte[] value5, byte[] family6,
                               byte[] column6, byte[] value6, byte[] family7, byte[] column7, byte[] value7, byte[] family8, byte[] column8, byte[] value8, byte[] family9
            , byte[] column9, byte[] value9, byte[] family10, byte[] column10, byte[] value10, byte[] family11, byte[] column11, byte[] value11, byte[] family12,
                                byte[] column12, byte[] value12, byte[] family13, byte[] column13, byte[] value13, byte[] family14,
                                byte[] column14, byte[] value14, byte[] family15, byte[] column15, byte[] value15, byte[] family16,
                                byte[] column16, byte[] value16, byte[] family17, byte[] column17, byte[] value17, byte[] family18,
                                byte[] column18, byte[] value18, byte[] family19, byte[] column19, byte[] value19, byte[] family20,
                                byte[] column20, byte[] value20, byte[] family21, byte[] column21, byte[] value21, byte[] family22,
                                byte[] column22, byte[] value22, byte[] family23, byte[] column23, byte[] value23, byte[] family24,
                                byte[] column24, byte[] value24, byte[] family25, byte[] column25, byte[] value25, byte[] family26,
                                byte[] column26, byte[] value26, byte[] family27, byte[] column27, byte[] value27, byte[] family28,
                                byte[] column28, byte[] value28, byte[] family29, byte[] column29, byte[] value29, byte[] family30,
                                byte[] column30, byte[] value30, byte[] family31, byte[] column31, byte[] value31, byte[] family32,
                                byte[] column32, byte[] value32, byte[] family33, byte[] column33, byte[] value33, byte[] family34,
                                byte[] column34, byte[] value34, byte[] family35, byte[] column35, byte[] value35, byte[] family36,
                                byte[] column36, byte[] value36) throws IOException {
        Put put = new Put(row);
        put.addColumn(family1, column1, value1);
        put.addColumn(family2, column2, value2);
        put.addColumn(family3, column3, value3);
        put.addColumn(family4, column4, value4);
        put.addColumn(family5, column5, value5);
        put.addColumn(family6, column6, value6);
        put.addColumn(family7, column7, value7);
        put.addColumn(family8, column8, value8);
        put.addColumn(family9, column9, value9);
        put.addColumn(family10, column10, value10);
        put.addColumn(family11, column11, value11);
        put.addColumn(family12, column12, value12);
        put.addColumn(family13, column13, value13);
        put.addColumn(family14, column14, value14);
        put.addColumn(family15, column15, value15);
        put.addColumn(family16, column16, value16);
        put.addColumn(family17, column17, value17);
        put.addColumn(family18, column18, value18);
        put.addColumn(family19, column19, value19);
        put.addColumn(family20, column20, value20);
        put.addColumn(family21, column21, value21);
        put.addColumn(family22, column22, value22);
        put.addColumn(family23, column23, value23);
        put.addColumn(family24, column24, value24);
        put.addColumn(family25, column25, value25);
        put.addColumn(family26, column26, value26);
        put.addColumn(family27, column27, value27);
        put.addColumn(family28, column28, value28);
        put.addColumn(family29, column29, value29);
        put.addColumn(family30, column30, value30);
        put.addColumn(family31, column31, value31);
        put.addColumn(family32, column32, value32);
        put.addColumn(family33, column33, value33);
        put.addColumn(family34, column34, value34);
        put.addColumn(family35, column35, value35);
        put.addColumn(family36, column36, value36);
        table.put(put);
    }

    /**
     * 删除一行数据
     * @param row
     * @throws IOException
     */
    public static void deleteData(Table table, byte[] row) throws IOException {
        // 删除一整行数据
        Delete delete = new Delete(row);
        table.delete(delete);
        /**
         * 删除一行中的某些列 Delete delete = new Delete(row); delete.deleteColumn(family,
         * column1); delete.deleteColumn(family, column2) table.delete(delete);
         */
    }

    /**
     * 查询一行数据
     * @param table
     * @param row
     * @throws IOException
     */
    public static Result getData(Table table, byte[] row) throws IOException {
        // 查询一整行数据
        Get get = new Get(row);
        Result result = table.get(get);
        return result;
        // 从result中获取某个列的值
        // result.getValue(family, column1);
        /**
         * 查询一行中的某些列 Get get = new Get(row); get.addColumn(family, column1);
         * get.addColumn(family, column2) Result result = table.get(get);
         */
    }

    /**
     * 扫描一个范围的数据
     * @param startRow
     * @param stopRow
     * @param limit
     * @throws IOException
     */
    public static void scanData(Table table, byte[] startRow, byte[] stopRow, int limit)
            throws IOException {
        Scan scan = new Scan(startRow, stopRow);
        //在扫描范围较小时，推荐使用small scanner
        scan.setSmall(true);
        /**
         * 只扫描某些列 scan.addColumn(family, column1); scan.addColumn(family,
         * column2);
         */
        ResultScanner scanner = table.getScanner(scan);
        int readCount = 0;
        for (Result result : scanner) {
            readCount++;
            // 处理查询结果result
            // ...
            System.out.println(result);
            if (readCount >= limit) {
                // 查询数量达到limit值，终止扫描
                break;
            }
        }
        // 最后需要关闭scanner
        scanner.close();
    }

    /**
     * 扫描一个前缀范围的数据
     * @throws IOException
     */
    public static void scanRowPrefixData(Table table, String rowKeyPrefix)
            throws IOException {
        Scan scan = new Scan();
        Scan filter = scan.setRowPrefixFilter(rowKeyPrefix.getBytes());
        //在扫描范围较小时，推荐使用small scanner
        ResultScanner scanner = table.getScanner(filter);
        for (Result result: scanner) {
            // 处理查询结果result
            for (Cell cell: result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + "   " +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        // 最后需要关闭scanner
        scanner.close();
    }

    /**
     * 创建表
     * @param tableName
     * @param family
     * @throws IOException
     */
    public static void createTable(Admin admin, String tableName, byte[] family) throws IOException {
        // 表的schema需要根据实际情况设置
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        htd.addFamily(new HColumnDescriptor(family));
        admin.createTable(htd);
    }

    /**
     * 停用表
     * @param tableName
     * @throws IOException
     */
    public static void disableTable(Admin admin, String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
    }

    /**
     * 启用表
     * @param tableName
     * @throws IOException
     */
    public static void enableTable(Admin admin, String tableName) throws IOException {
        admin.enableTable(TableName.valueOf(tableName));
    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(Admin admin, String tableName) throws IOException {
        admin.deleteTable(TableName.valueOf(tableName));
    }

}
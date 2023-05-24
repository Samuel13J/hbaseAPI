package org.leelen.hbase;

/**
 * @author wangjie
 * @create 2023-05-10 11:56
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseAPI {

    private static Configuration conf;
    private static Connection connection = null;

    static {
//        新建一个Configuration
        conf = HBaseConfiguration.create();
//        集群的连接地址(VPC内网地址)在控制台页面的数据库连接界面获得
//        conf.set("hbase.zookeeper.quorum", "ld-bp19x2n530lt779sd-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30020");
//        conf.set("hbase.zookeeper.quorum", "ld-bp19x2n530lt779sd-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30060");
        conf.set("hbase.zookeeper.quorum", "ld-bp19x2n530lt779sd-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30060");

//        xml_template.comment.hbaseue.username_password.default
        conf.set("hbase.client.username", "admin");
        conf.set("hbase.client.password", "admin");
//        如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Result getData(String tableName, String rowKey, Integer startRow, Integer endRow) throws IOException {

        //Table为非线程安全对象，每个线程在对Table操作时，都必须从Connection中获取相应的Table对象
        Result res = null;
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {


            // 插入数据
//            Put put = new Put(Bytes.toBytes("row"));
//            put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
//            table.put(put);

//            //单行读取
            Get get = new Get(Bytes.toBytes(rowKey));
            res = table.get(get);
//
//            //scan范围数据
//            Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
//            ResultScanner scanner = table.getScanner(scan);
////            for (Result result : scanner) {
////                //处理查询结果result
////                // ...
////            }
//            scanner.close();

//            Scan scan = new Scan();
//            scan.addColumn(Bytes.toBytes("state"), Bytes.toBytes("desired.color"));
//            scan.setStartRow(Bytes.toBytes("device1_1462232313_"));
//            scan.setStopRow(Bytes.toBytes("device1_1748271841_"));
//            ResultScanner scanner = table.getScanner(scan);
//            for (Result result: scanner) {
//                System.out.println(result);
//            }
        }
        return res;

    }

    public static void main(String[] args) throws IOException {
        Result result = getData("leelen_iot:leelen_iot_device_properties", "productKey1_deviceName1_1680744897012_v0", 1, 100);
        System.out.println(result);
    }
}

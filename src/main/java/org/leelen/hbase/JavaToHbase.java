package org.leelen.hbase;


import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangjie
 * @create 2023-05-31 9:21
 */
public class JavaToHbase {

    private static final Logger logger = LoggerFactory.getLogger(JavaToHbase.class);
    public static Configuration configuration = null;
    public static Connection conn = null;
    static {
        // 获得配制文件对象
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "ld-bp19x2n530lt779sd-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30020");
        configuration.set("hbase.client.username", "admin");
        configuration.set("hbase.client.password", "admin");
//        如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        configuration.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
        // 线程池，500线程插入
        ExecutorService threads = Executors.newFixedThreadPool(500);

        try {
            conn = ConnectionFactory.createConnection(configuration, threads);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    /**
     * 批量插入数据
     *
     * @return
     * @throws ParseException
     * @tableName 表名字
     * @info 需要写入的信息
     */
    public static void putList(String tableName) throws IOException, ParseException {

        try {
//            缓冲区，数据异步批量的将数据写入hbase表
            BufferedMutator table = conn.getBufferedMutator(TableName.valueOf(tableName));
            try {
                long rowkeys = 1661698512451588205L;
                for (int i=0; i<20000000; i++){
                    rowkeys = rowkeys + 1;
                    System.out.println(rowkeys);
                    Put put = new Put(String.valueOf(rowkeys).getBytes());
                    put.addColumn("master".getBytes(), "link_id".getBytes(), "1".getBytes());
                    put.addColumn("master".getBytes(), "link_type".getBytes(), "0".getBytes());
                    put.addColumn("master".getBytes(), "icon".getBytes(), "1".getBytes());
                    put.addColumn("master".getBytes(), "sync_result".getBytes(), "0".getBytes());
                    put.addColumn("master".getBytes(), "enabled".getBytes(), "0".getBytes());
                    put.addColumn("master".getBytes(), "condition_relation".getBytes(), "1".getBytes());
                    put.addColumn("master".getBytes(), "link_app_type".getBytes(), "1".getBytes());
                    put.addColumn("master".getBytes(), "effect_time".getBytes(), "1685014771987".getBytes());
                    put.addColumn("master".getBytes(), "expire_time".getBytes(), "1685014771987".getBytes());
                    put.addColumn("master".getBytes(), "timer_type".getBytes(), "1".getBytes());
                    put.addColumn("master".getBytes(), "customize_time".getBytes(), "1685014771987".getBytes());
                    put.addColumn("item".getBytes(), "do_did".getBytes(), "q93f9fAN4HF2".getBytes());
                    put.addColumn("item".getBytes(), "trigger_link_obj".getBytes(), "q93f9fAN4HF2".getBytes());
                    put.addColumn("item".getBytes(), "local_status".getBytes(), "q93f9fAN4HF2".getBytes());
                    put.addColumn("item".getBytes(), "create_by".getBytes(), "q93f9fAN4HF2".getBytes());
                    put.addColumn("item".getBytes(), "create_time".getBytes(), "1685014771987".getBytes());
                    put.addColumn("item".getBytes(), "update_by".getBytes(), "1685014771987".getBytes());
                    put.addColumn("item".getBytes(), "update_time".getBytes(), "1685014771987".getBytes());
                    put.addColumn("action".getBytes(), "ctrl_id".getBytes(), "1".getBytes());
                    put.addColumn("action".getBytes(), "ctrl_type".getBytes(), "1".getBytes());
                    put.addColumn("action".getBytes(), "scene_id".getBytes(), "0".getBytes());
                    put.addColumn("action".getBytes(), "direct_did".getBytes(), "0".getBytes());
                    put.addColumn("action".getBytes(), "did".getBytes(), "0".getBytes());
                    put.addColumn("action".getBytes(), "siid".getBytes(), "1685014771987".getBytes());
                    put.addColumn("action".getBytes(), "fiids".getBytes(), "1685014771987".getBytes());
                    put.addColumn("action".getBytes(), "is_update".getBytes(), "1685014771987".getBytes());
                    put.addColumn("action".getBytes(), "ctrl_direct_did".getBytes(), "1".getBytes());
                    put.addColumn("action".getBytes(), "ctrl_did".getBytes(), "1".getBytes());
                    put.addColumn("action".getBytes(), "ctrl_siid".getBytes(), "1".getBytes());
                    put.addColumn("action".getBytes(), "ctrl_fiids".getBytes(), "1685014771987".getBytes());
                    put.addColumn("action".getBytes(), "default_name".getBytes(), "1685014771987".getBytes());
                    put.addColumn("action".getBytes(), "ext_info".getBytes(), "1".getBytes());
                    put.addColumn("action".getBytes(), "service_type".getBytes(), "1".getBytes());
                    put.addColumn("action".getBytes(), "efficacy_status".getBytes(), "1".getBytes());
                    put.addColumn("action".getBytes(), "biz_id".getBytes(), "1685014771987".getBytes());
                    put.addColumn("action".getBytes(), "sort".getBytes(), "1685014771987".getBytes());

                    List<Mutation> mutations = new ArrayList<Mutation>();
                    mutations.add(put);
                    table.mutate(mutations);
                }
            } finally {
                if (table != null) {
                    table.close();
                }
            }
        } finally {
            conn.close();
        }
    }

    /**
     * 生成随机码
     *
     */
    public static String getRowkeyByUUId(String CRAD_NUMBER, String TRADING_TIME_STAMP) throws ParseException {

        String[] chars = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
                "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A",
                "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
                "W", "X", "Y", "Z" };

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 19 - CRAD_NUMBER.length(); i++) {
            sb.append("0");
        }
        sb.append(CRAD_NUMBER);
        CRAD_NUMBER = sb.toString();
        int machineId = new Random().nextInt(9) % (9 - 1 + 1) + 1;
        StringBuffer shortBuffer = new StringBuffer();
        String uuid = UUID.randomUUID().toString().replace("-", "");
        for (int i = 0; i < 8; i++) {
            String str = uuid.substring(i * 3, i * 3 + 4);
            int x = Integer.parseInt(str, 16);
            shortBuffer.append(chars[x % 0x3E]);
        }
        return machineId + shortBuffer.toString() + CRAD_NUMBER + TRADING_TIME_STAMP;
    }

    public static void main(String[] args) throws IOException, ParseException {
//        主函数调用
        putList("leelen_iot:home_linkage_wide");
    }
}

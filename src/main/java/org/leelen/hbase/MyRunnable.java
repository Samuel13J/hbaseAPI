package org.leelen.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wangjie
 * @create 2023-05-30 10:55
 */
public class MyRunnable implements Runnable{
    private static Configuration conf;
    private static Connection connection = null;


    static {
        ExecutorService threads = Executors.newFixedThreadPool(200);
//        新建一个Configuration
        conf = HBaseConfiguration.create();
//        集群的连接地址(VPC内网地址)在控制台页面的数据库连接界面获得
//        conf.set("hbase.zookeeper.quorum", "ld-bp19x2n530lt779sd-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30020");
        conf.set("hbase.zookeeper.quorum", "ld-bp19x2n530lt779sd-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30020");
        conf.set("hbase.client.username", "admin");
        conf.set("hbase.client.password", "admin");
//        如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
        try {
            connection = ConnectionFactory.createConnection(conf, threads);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void run() {

        BufferedMutator table = null;
        try {
            table = connection.getBufferedMutator(TableName.valueOf("leelen_iot:aasd"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long rowKey = 162998365476433684L;
        for (int i = 0; i<20000000; i++) {
            rowKey = rowKey + 1;
            System.out.println(rowKey);
            try {
                Put put = new Put(String.valueOf(rowKey).getBytes());
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
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }


        public static void main(String[] args) throws IOException {
            MyRunnable myRun = new MyRunnable();//将一个任务提取出来，让多个线程共同去执行
            //封装线程对象
            for (int i = 1; i < 200; i++) {
                Thread t = new Thread(myRun, "线程" + i);
                //开启线程
                t.start();
            }
            //通过匿名内部类的方式创建线程
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 20; i++) {
                        System.out.println(Thread.currentThread().getName() + " - " + i);
                    }
                }
            },"线程04").start();
//            MyRunnable myRun = new MyRunnable();
//            myRun.run();
    }

}

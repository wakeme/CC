package com.wakeme.homework3;
 
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
 
//从hdfs读取文件存到hbase
public class Main {
 
    public static void main(String[] args) throws Exception {
        System.exit(run());
    }
 
    public static int run() throws Exception {
        Configuration conf = new Configuration();
        conf = HBaseConfiguration.create(conf);
    	conf.set("hbase.zookeeper.property.clientPort", "2181"); 
    	conf.set("hbase.zookeeper.quorum", "192.168.31.171"); 
    	conf.set("hbase.master", "192.168.31.171:16010"); //TO-DO
 
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(Main.class);
 
        job.setInputFormatClass(KeyValueTextInputFormat.class);
 
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path("out/part-r-00000"));
        // 把数据写入Hbase数据库
 
        TableMapReduceUtil.initTableReducerJob("word", MyHbaseReducer.class, job);
        checkTable(conf);
        return job.waitForCompletion(true) ? 0 : 1;
 
    }
    //创建表
    private static void checkTable(Configuration conf) throws Exception {
        Connection con = ConnectionFactory.createConnection(conf);
        Admin admin = con.getAdmin();
        TableName tn = TableName.valueOf("word");
        if (admin.tableExists(tn)){
        	admin.disableTable(tn);
        	admin.deleteTable(tn);
        }
        HTableDescriptor htd = new HTableDescriptor(tn);
        HColumnDescriptor hcd = new HColumnDescriptor("wordcount");
        htd.addFamily(hcd);
        admin.createTable(htd);
        System.out.println("[Successful] create table word");
//        }
    }
    //把数据存到hbase
    public static class MyHbaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable>{
 
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
            // T一定要先tostring然后再转为byte,不然会词会有点不准确
 
            Put put=new Put(key.toString().getBytes());
 
            put.addColumn(Bytes.toBytes("wordcount"), Bytes.toBytes("num"), values.iterator().next().getBytes());
 
            context.write(new ImmutableBytesWritable(key.getBytes()), put);
        }
    }
}
package base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * 预分区概念
 * HBase默认建表时有一个region 这个region的rowkey是没有边界的 即没有startkey和endkey
 * 在数据写入时 所有数据都会写入这个默认的region 随着数据量的不断增加 此region已经不能承受不断增长的数据量 会进行split 分成2个region
 * 在此过程中 会产生两个问题
 * 1.数据往一个region上写 会有写热点问题
 * 2.region split会消耗宝贵的集群I/O资源 基于此我们可以控制在建表的时候 创建多个空region 并确定每个region的起始和终止rowky 
 * 这样只要我们的rowkey设计能均匀的命中各个region 就不会存在写热点问题 自然split的几率也会大大降低 
 * 
 * 当然随着数据量的不断增长 该split的还是要进行split 像这样预先创建hbase表分区的方式 称之为预分区
 * 
 * 此外 hmaster会定期检查regionserver上的region个数 确定平均值后进行负载均衡
 * @author summerKiss
 *
 */

public class TestRegion {
	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("t4"));
		HColumnDescriptor hcd = new HColumnDescriptor("cf1".getBytes());
		htd.addFamily(hcd);
		
		/*
		 * 第一种方式
		 * startkey 是第一个分区的结束rowkey 
		 * endKey 是最后一个分区的起始rowkey 
		 * 中间的分区是100~500平均分的
		 */
		//admin.createTable(htd, "100".getBytes(), "500".getBytes(), 6);
		
		
		/* 
		 * 第二种方式 
		 * 二维数组定义的是region间的分界线
		 * */
		
		byte[][] b = {"100".getBytes(),"200".getBytes(),"300".getBytes()};
		admin.createTableAsync(htd, b);
		
		conn.close();
		
	}
}

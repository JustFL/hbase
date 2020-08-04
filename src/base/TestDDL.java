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

public class TestDDL {
	public static void main(String[] args) throws IOException {
		
		//获取配置文件 并且设置zookeeper的访问地址
		Configuration conf = HBaseConfiguration.create();
		
		//配置hbase集群的zookeeper的地址
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");
		
		//创建连接对象
		Connection connect = ConnectionFactory.createConnection(conf);
		
		//创建ddl操作的admin对象
		Admin admin = connect.getAdmin();
		
		//创建表描述器对象 用于创建表
		//HTableDescriptor des = new HTableDescriptor("t1".getBytes());
		HTableDescriptor des = new HTableDescriptor(TableName.valueOf("t2"));
		
		//创建表的时候至少需要一个列簇描述器
		HColumnDescriptor hc = new HColumnDescriptor("c1");
		
		//将列簇描述器加载到表描述器上
		des.addFamily(hc);
		
		//创建表
		admin.createTable(des);
		
		//删除表的列簇
		/*
		 * HColumnDescriptor hcd = new HColumnDescriptor("c2");
		 * admin.addColumn(TableName.valueOf("t2"), hcd);
		 * admin.deleteColumn(TableName.valueOf("t2"), "c2".getBytes());
		 */
		
		//删除表
		if (admin.tableExists(TableName.valueOf("t3"))) {
			if (admin.isTableEnabled(TableName.valueOf("t3"))) {
				admin.disableTable(TableName.valueOf("t3"));
			}
			admin.deleteTable(TableName.valueOf("t3"));
		}
		
		//列出所有的列表信息
		TableName[] TableNames = admin.listTableNames();
		for (TableName tableName : TableNames) {
			System.out.println(tableName);
		}

		
		//关闭连接
		connect.close();
	}
}

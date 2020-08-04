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
		
		//��ȡ�����ļ� ��������zookeeper�ķ��ʵ�ַ
		Configuration conf = HBaseConfiguration.create();
		
		//����hbase��Ⱥ��zookeeper�ĵ�ַ
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");
		
		//�������Ӷ���
		Connection connect = ConnectionFactory.createConnection(conf);
		
		//����ddl������admin����
		Admin admin = connect.getAdmin();
		
		//���������������� ���ڴ�����
		//HTableDescriptor des = new HTableDescriptor("t1".getBytes());
		HTableDescriptor des = new HTableDescriptor(TableName.valueOf("t2"));
		
		//�������ʱ��������Ҫһ���д�������
		HColumnDescriptor hc = new HColumnDescriptor("c1");
		
		//���д����������ص�����������
		des.addFamily(hc);
		
		//������
		admin.createTable(des);
		
		//ɾ������д�
		/*
		 * HColumnDescriptor hcd = new HColumnDescriptor("c2");
		 * admin.addColumn(TableName.valueOf("t2"), hcd);
		 * admin.deleteColumn(TableName.valueOf("t2"), "c2".getBytes());
		 */
		
		//ɾ����
		if (admin.tableExists(TableName.valueOf("t3"))) {
			if (admin.isTableEnabled(TableName.valueOf("t3"))) {
				admin.disableTable(TableName.valueOf("t3"));
			}
			admin.deleteTable(TableName.valueOf("t3"));
		}
		
		//�г����е��б���Ϣ
		TableName[] TableNames = admin.listTableNames();
		for (TableName tableName : TableNames) {
			System.out.println(tableName);
		}

		
		//�ر�����
		connect.close();
	}
}

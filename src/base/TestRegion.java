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
 * Ԥ��������
 * HBaseĬ�Ͻ���ʱ��һ��region ���region��rowkey��û�б߽�� ��û��startkey��endkey
 * ������д��ʱ �������ݶ���д�����Ĭ�ϵ�region �����������Ĳ������� ��region�Ѿ����ܳ��ܲ��������������� �����split �ֳ�2��region
 * �ڴ˹����� �������������
 * 1.������һ��region��д ����д�ȵ�����
 * 2.region split�����ı���ļ�ȺI/O��Դ ���ڴ����ǿ��Կ����ڽ����ʱ�� ���������region ��ȷ��ÿ��region����ʼ����ֹrowky 
 * ����ֻҪ���ǵ�rowkey����ܾ��ȵ����и���region �Ͳ������д�ȵ����� ��Ȼsplit�ļ���Ҳ���󽵵� 
 * 
 * ��Ȼ�����������Ĳ������� ��split�Ļ���Ҫ����split ������Ԥ�ȴ���hbase������ķ�ʽ ��֮ΪԤ����
 * 
 * ���� hmaster�ᶨ�ڼ��regionserver�ϵ�region���� ȷ��ƽ��ֵ����и��ؾ���
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
		 * ��һ�ַ�ʽ
		 * startkey �ǵ�һ�������Ľ���rowkey 
		 * endKey �����һ����������ʼrowkey 
		 * �м�ķ�����100~500ƽ���ֵ�
		 */
		//admin.createTable(htd, "100".getBytes(), "500".getBytes(), 6);
		
		
		/* 
		 * �ڶ��ַ�ʽ 
		 * ��ά���鶨�����region��ķֽ���
		 * */
		
		byte[][] b = {"100".getBytes(),"200".getBytes(),"300".getBytes()};
		admin.createTableAsync(htd, b);
		
		conn.close();
		
	}
}

package base;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class TestPageFilter {
	public static void main(String[] args) throws IOException {
		
		ResultScanner data = getPageData(4, 4);
		Iterator<Result> iterator = data.iterator();
		while (iterator.hasNext()) {
			Result next = iterator.next();
			String row = new String(next.getRow());
			System.out.println(row);
		}
	}
	
	
	/*
	 * ����getData����
	 * �������ֻ��Ҫ�����ǰpageindexʱ����ʼrowkey
	 * */
	
	public static ResultScanner getPageData(int pageindex, int pagesize) throws IOException {
		
		if (pageindex <= 1) {
			ResultScanner data = getData("",pagesize);
			return data;
		}else {
			
			String rowkey = "";
			
			//ѭ������֮ǰ��ÿһҳ ��ȡÿһҳ�����һ��rowkey �ӵ�һҳ��ʼѭ������
			for (int i = 0; i < pageindex - 1; i++) {
				
				ResultScanner data = getData(rowkey, pagesize);
				Iterator<Result> iter = data.iterator();
				//whileѭ������ rowkeyΪ��ǰpage���һ��rowkey
				while (iter.hasNext()) {
					rowkey = new String(iter.next().getRow());
				}
				
				//����һ�� ��Ϊ��һҳ����ʼrowkey
				rowkey = new String(Bytes.add(rowkey.getBytes(), "00x0".getBytes()));
			}
			
			//��forѭ������ ���������ҳ�����ʼrowkey
			ResultScanner data = getData(rowkey, pagesize);
			return data;
		}
	}
	
	public static ResultScanner getData(String rowkey, int pagesize) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");
				
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf("student"));
		
		Scan scan = new Scan();
		
		if(!StringUtils.isBlank(rowkey)){
			scan.setStartRow(rowkey.getBytes());
		}

		Filter f = new PageFilter(pagesize);
		scan.setFilter(f);
		
		ResultScanner scanner = table.getScanner(scan);
		return scanner;
	}
}

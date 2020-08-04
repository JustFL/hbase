package base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDML {
	
	Configuration conf = null;
	Connection connect = null;
	Table table = null;
	
	public static void main(String[] args) throws IOException {}
	
	@Before
	public void init() throws IOException {
		//��ȡ�����ļ� ��������zookeeper�ķ��ʵ�ַ
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");
				
		//�������Ӷ���
		connect = ConnectionFactory.createConnection(conf);
	}
	
	
	@Test
	public void put() throws IOException {
		
		//��ȡ����� ���������ԶԱ�����ݽ��в���
		table = connect.getTable(TableName.valueOf("t1"));
		
		//����put���� ��������дض���
		Put p1 = new Put("rk07".getBytes());
		p1.addColumn("cf2".getBytes(), "age".getBytes(), "10".getBytes());
		
		//�������
		table.put(p1);
	}
	
	@Test
	public void mutiPut() throws IOException {
		
		//��ȡ����� ���������ԶԱ�����ݽ��в���
		table = connect.getTable(TableName.valueOf("t1"));
		
		//��Ӷ�������
		List<Put> list = new ArrayList<>(); 
		for (int i = 3; i < 7; i++) { 
			Put put = new Put(("rk0"+i).getBytes()); 
			put.addColumn("cf1".getBytes(),"name".getBytes(), ("wangzulan"+i).getBytes()); 
			list.add(put); 
		}
	  
		table.put(list);
	}
	
	@Test
	public void delete() throws IOException {
		
		table = connect.getTable(TableName.valueOf("t1"));
		
		//ɾ������		
		Delete d1 = new Delete("rk06".getBytes());
		d1.addColumn("cf1".getBytes(),"name".getBytes()); 
		table.delete(d1);
	}
	
	@Test
	public void get() throws IOException {
		
		table = connect.getTable(TableName.valueOf("t1"));
		
		//��ѯget
		Get g1 = new Get("rk01".getBytes());
		Result result = table.get(g1);
		List<KeyValue> list = result.list();
		for (KeyValue kv : list) {
			String f = Bytes.toString(kv.getFamily());
			String q = Bytes.toString(kv.getQualifier());
			String v = new String(kv.getValue());
			System.out.print(f + "\t" + q + "\t"+ v);
			System.out.println();
		}
	}
	
	@Test
	public void scan() throws IOException {
		
		table = connect.getTable(TableName.valueOf("t1"));
		
		//ȫ��ɨ�����
		Scan scan = new Scan();
		
		//����scan�ķ�Χ
		//scan.addFamily("cf1".getBytes());
		
		//�������������� ��������
		RowFilter rkFilter = new RowFilter(CompareOp.GREATER, new BinaryComparator("rk03".getBytes()));
		ValueFilter ageFilter = new ValueFilter(CompareOp.LESS, new BinaryComparator("30".getBytes()));
		//�����������б���� �ں������������
		FilterList filterList = new FilterList(rkFilter,ageFilter);
		
		//���ù�����
		scan.setFilter(filterList);
		
		//��ȡȫ��ɨ������
		ResultScanner scanner = table.getScanner(scan);
		
		Iterator<Result> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Result next = iterator.next();
			List<KeyValue> list = next.list();
			for (KeyValue kv : list) {
				String k = new String(kv.getRow());
				String f = Bytes.toString(kv.getFamily());
				String q = Bytes.toString(kv.getQualifier());
				String v = new String(kv.getValue());
				System.out.print(k + "\t" + f + "\t" + q + "\t"+ v);
				System.out.println();
			}
		}
	}
	
	@Test
	public void rangeScan() throws IOException {
		
		table = connect.getTable(TableName.valueOf("t1"));
		
		//ȫ��ɨ�����
		Scan scan = new Scan();
		scan.setStartRow("rk03".getBytes());
		scan.setStopRow("rk04".getBytes());
		
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Result next = iterator.next();
			List<KeyValue> list = next.list();
			for (KeyValue kv : list) {
				String k = new String(kv.getRow());
				String f = Bytes.toString(kv.getFamily());
				String q = Bytes.toString(kv.getQualifier());
				String v = new String(kv.getValue());
				System.out.print(k + "\t" + f + "\t" + q + "\t"+ v);
				System.out.println();
			}
		}
	}
	
	
	@After
	public void finalize() throws IOException {
		connect.close();
	}
}

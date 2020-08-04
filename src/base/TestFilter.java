package base;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class TestFilter {
	
	Configuration conf = null;
	Connection connect = null;
	Table table = null;
	
	public static void main(String[] args) throws IOException {}
	
	@Before
	public void init() throws IOException {

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");
				
		connect = ConnectionFactory.createConnection(conf);
		table = connect.getTable(TableName.valueOf("student"));
	}
	
	
	@Test
	public void filterlist() throws IOException {
		
		Scan scan = new Scan();
		
		//�й�����
		Filter f1 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator("name".getBytes()));
		//�дع�����
		Filter f2 = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator("cf1".getBytes()));
		//��϶�������� ���ҿ�����Ӷ������֮����߼���&& ���� ||
		FilterList fl = new FilterList(Operator.MUST_PASS_ONE, f1, f2);
		//���ù�����
		scan.setFilter(fl);
		//�ύ��ѯ
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
	public void testSingleColumnValueFilter() throws IOException {
		
		Scan scan = new Scan();
		//����ֵ������
		SingleColumnValueFilter f1 = new SingleColumnValueFilter("cf1".getBytes(), "name".getBytes(), CompareOp.EQUAL, "��С��".getBytes());
		//���ù����� �������и��е��й��˵�
		f1.setFilterIfMissing(true);
		scan.setFilter(f1);
		//�ύ��ѯ
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
	public void testPrefixFilter() throws IOException {
		Scan scan = new Scan();
		
		//�м�ǰ׺������
		Filter f1 = new PrefixFilter("95".getBytes());
		//��ǰ׺������
		Filter f2 = new ColumnPrefixFilter("d".getBytes());
		//��϶��������
		FilterList fl = new FilterList(f1, f2);
		//���ù�����
		scan.setFilter(fl);
		//�ύ��ѯ

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
}

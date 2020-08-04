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
		
		//列过滤器
		Filter f1 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator("name".getBytes()));
		//列簇过滤器
		Filter f2 = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator("cf1".getBytes()));
		//组合多个过滤器 而且可以添加多个条件之间的逻辑是&& 或者 ||
		FilterList fl = new FilterList(Operator.MUST_PASS_ONE, f1, f2);
		//设置过滤器
		scan.setFilter(fl);
		//提交查询
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
		//单列值过滤器
		SingleColumnValueFilter f1 = new SingleColumnValueFilter("cf1".getBytes(), "name".getBytes(), CompareOp.EQUAL, "邢小丽".getBytes());
		//设置过滤器 将不含有该列的行过滤掉
		f1.setFilterIfMissing(true);
		scan.setFilter(f1);
		//提交查询
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
		
		//行键前缀过滤器
		Filter f1 = new PrefixFilter("95".getBytes());
		//列前缀过滤器
		Filter f2 = new ColumnPrefixFilter("d".getBytes());
		//组合多个过滤器
		FilterList fl = new FilterList(f1, f2);
		//设置过滤器
		scan.setFilter(fl);
		//提交查询

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

package loaddata;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReadHbaseToHdfs {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://myha/");
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");
		
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadHbaseToHdfs.class);
		
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("student".getBytes(), 
				scan, 
				MyMapper.class, 
				NullWritable.class, 
				IntWritable.class, 
				job, 
				false);// «∑ÒÃÌº”ª∫¥Ê“¿¿µ
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/hbase_out01"));
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends TableMapper<NullWritable, IntWritable>{
		
		IntWritable v = new IntWritable();
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, NullWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			List<KeyValue> list = value.list();
			for (KeyValue kv : list) {
				String Qualifier = new String(kv.getQualifier());
				if (Qualifier.equals("age")) {
					String str = new String(kv.getValue());
					v.set(Integer.parseInt(str));
					context.write(NullWritable.get(), v);
				}
			}
		}
	}
	
	public static class MyReducer extends Reducer<NullWritable, IntWritable, NullWritable, DoubleWritable>{
		@Override
		protected void reduce(NullWritable key, Iterable<IntWritable> values,
				Reducer<NullWritable, IntWritable, NullWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			int sum = 0;
			for (IntWritable i : values) {
				count++;
				sum+=i.get();
			}
			double d = (double)sum/count;
			context.write(NullWritable.get(), new DoubleWritable(d));
		}
	}
}

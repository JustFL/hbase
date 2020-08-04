package loaddata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LoadHdfsToHbase {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.setProperty("HADOOP_USER_NAME", "HADOOP");
		
		Configuration conf = new Configuration(); 
		//集群模式下 设置hdfs地址
		conf.set("fs.defaultFS", "hdfs://myha/");
		//设置zookeeper地址
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");
		
		Job job = Job.getInstance(conf, "ToHbase");
		job.setJarByClass(LoadHdfsToHbase.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		TableMapReduceUtil.initTableReducerJob("student", MyReducer.class, job, null, null, null, null, false);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Mutation.class);
		
		FileInputFormat.addInputPath(job, new Path("/stu"));
		job.waitForCompletion(true);
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}
	
	static class MyReducer extends TableReducer<Text, NullWritable, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

			for (NullWritable nw : values) {
				System.out.println(nw);
				String[] split = key.toString().split(",");
				Put p = new Put(split[0].getBytes());
				p.addColumn("cf1".getBytes(), "name".getBytes(), split[1].getBytes());
				p.addColumn("cf1".getBytes(), "age".getBytes(), split[3].getBytes());
				p.addColumn("cf1".getBytes(), "sex".getBytes(), split[2].getBytes());
				p.addColumn("cf1".getBytes(), "department".getBytes(), split[4].getBytes());
				
			   context.write(NullWritable.get(), p);
			}
		}
	}
}

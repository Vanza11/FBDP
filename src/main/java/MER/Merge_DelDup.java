package MER;
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Merge_DelDup {

	/*
	 * 利用key不会重复的原理
	 */
	//将输入中的value作为输出数据的key上
	public static class Map extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			context.write(value, new Text("AB"));
		}
	}
	
	//直接将输入中的key复制到输出数据的key上
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException,InterruptedException{
			context.write(key, new Text(""));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		
		Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");
		String[] otherArgs = new String[]{"Homework_5","output"}; 
		if (otherArgs.length != 2) {
			System.err.println("Usage: merge and delete duplicate lines");
			System.exit(2);
			}
		Job job = Job.getInstance(conf,"Merge and Delete");
		job.setJarByClass(Merge_DelDup.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

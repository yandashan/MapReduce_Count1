package cn.dataCount.mr.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapRedece_ipCount_first {

	public static class MapRedece_ipCount_firstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String[] split = line.split(",");

			context.write(new Text(split[0] + "\t" + split[5]), new IntWritable(1));

		}

	}

	public static class MapRedece_ipCount_firstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;

			for (IntWritable value : values) {

				count += value.get();

			}

			context.write(key, new IntWritable(count));

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("MapRedece_ipCount_first");
		job.setJarByClass(MapRedece_ipCount_first.class);

		// map设置
		job.setMapperClass(MapRedece_ipCount_firstMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reduce设置
		job.setReducerClass(MapRedece_ipCount_firstReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 文件输入路径与输出路径
		FileInputFormat.setInputPaths(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogClean/Result/part-r-00000"));
		FileOutputFormat.setOutputPath(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogResult/Result_ipCount"));

		// 设置reduce个数
		job.setNumReduceTasks(1);

		// 任务提示
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag);
		System.exit(flag ? 0 : 1);
	}
}

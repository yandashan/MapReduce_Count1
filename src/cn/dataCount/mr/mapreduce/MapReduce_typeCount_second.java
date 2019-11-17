package cn.dataCount.mr.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce_typeCount_second {
	public static class MapReduce_typeCount_secondMapper extends Mapper<LongWritable, Text, PageCount, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// String line = value.toString();

			String[] split = value.toString().split("\t");

			PageCount pageCount = new PageCount(split[0],split[1], Integer.parseInt(split[2]));

			context.write(pageCount, NullWritable.get());

		}

	}

	public static class MapReduce_typeCount_secondReducer
			extends Reducer<PageCount, NullWritable, PageCount, NullWritable> {

		@Override
		protected void reduce(PageCount key, Iterable<NullWritable> values,
				Reducer<PageCount, NullWritable, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("MapReduce_typeCount_second");
		job.setJarByClass(MapReduce_typeCount_second.class);

		// map设置
		job.setMapperClass(MapReduce_typeCount_secondMapper.class);
		job.setMapOutputKeyClass(PageCount.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reduce设置
		job.setReducerClass(MapReduce_typeCount_secondReducer.class);
		job.setOutputKeyClass(PageCount.class);
		job.setOutputValueClass(NullWritable.class);

		// 文件输入路径与输出路径
		FileInputFormat.setInputPaths(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogResult/Result_typeCount/part-r-00000"));
		FileOutputFormat.setOutputPath(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogResult/Result_typeCount/Result"));

		// 任务提示
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag);
		System.exit(flag ? 0 : 1);
	}

}

package cn.dataClean.mr.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AccessLogCleanJob {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
//		 // 判断输入输出路径
//		 if (args == null || args.length < 2) {
//		 System.err.println("Parameter Errors!Usage<inputPath...><outputPath>");
//		 System.exit(-1);
//		 }

		Job job = Job.getInstance(conf);
		job.setJobName("AccessLogCleanJob");
		job.setJarByClass(AccessLogCleanJob.class);

		// map设置
		job.setMapperClass(AccessLogCleanMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AccessLogWritable.class);

		// reduce设置
		job.setReducerClass(AccessLogCleanReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AccessLogWritable.class);

		// 文件输入路径与输出路径
		FileInputFormat.setInputPaths(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogClean/result.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogClean/Result"));

		// 任务提示
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag);
		System.exit(flag ? 0 : 1);
	}
}

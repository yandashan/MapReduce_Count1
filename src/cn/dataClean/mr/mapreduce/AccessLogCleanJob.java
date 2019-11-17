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
//		 // �ж��������·��
//		 if (args == null || args.length < 2) {
//		 System.err.println("Parameter Errors!Usage<inputPath...><outputPath>");
//		 System.exit(-1);
//		 }

		Job job = Job.getInstance(conf);
		job.setJobName("AccessLogCleanJob");
		job.setJarByClass(AccessLogCleanJob.class);

		// map����
		job.setMapperClass(AccessLogCleanMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AccessLogWritable.class);

		// reduce����
		job.setReducerClass(AccessLogCleanReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AccessLogWritable.class);

		// �ļ�����·�������·��
		FileInputFormat.setInputPaths(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogClean/result.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogClean/Result"));

		// ������ʾ
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag);
		System.exit(flag ? 0 : 1);
	}
}

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

public class MapReduce_ipCount_second {
	public static class MapReduce_ipCount_secondMapper extends Mapper<LongWritable, Text, PageCount, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// String line = value.toString();

			String[] split = value.toString().split("\t");

			PageCount pageCount = new PageCount(split[0], split[1], Integer.parseInt(split[2]));

			context.write(pageCount, NullWritable.get());

		}

	}

	public static class MapReduce_ipCount_secondReducer
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
		job.setJobName("MapReduce_ipCount_second");
		job.setJarByClass(MapReduce_ipCount_second.class);

		// map����
		job.setMapperClass(MapReduce_ipCount_secondMapper.class);
		job.setMapOutputKeyClass(PageCount.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reduce����
		job.setReducerClass(MapReduce_ipCount_secondReducer.class);
		job.setOutputKeyClass(PageCount.class);
		job.setOutputValueClass(NullWritable.class);

		// �ļ�����·�������·��
		FileInputFormat.setInputPaths(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogResult/Result_ipCount/part-r-00000"));
		FileOutputFormat.setOutputPath(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogResult/Result_ipCount/Result"));

		// ������ʾ
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag);
		System.exit(flag ? 0 : 1);
	}
}

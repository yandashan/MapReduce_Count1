package cn.dataClean.mr.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AccessLogCleanReduce extends Reducer<Text, AccessLogWritable, NullWritable, AccessLogWritable> {

	String ip;
	String time;
	String day;
	String traffic;
	String type;
	String id;
	String ip_sum;

	// ÷ÿ–¥reduce∑Ω∑®
	@Override
	protected void reduce(Text key, Iterable<AccessLogWritable> values, Context context)
			throws IOException, InterruptedException {
		for (AccessLogWritable value : values) {// <init>
			time = value.getTime();
			day = value.getDay();
			traffic = value.getTraffic();
			type = value.getType();
			id = value.getId();
			System.out.println(id);
			context.write(NullWritable.get(), new AccessLogWritable(key.toString(), time, day, traffic, type, id));
		}

	}

}

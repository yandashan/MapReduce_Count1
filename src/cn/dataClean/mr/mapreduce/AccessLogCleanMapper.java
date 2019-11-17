package cn.dataClean.mr.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 清洗数据时的mapper类
 * 
 * @author Lenovo
 *
 */
public class AccessLogCleanMapper extends Mapper<LongWritable, Text, Text, AccessLogWritable> {

	// 设置本文件时间格式和目标文件格式，用于时间格式的转变
	public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH); // 原时间格式
	public static final SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 期望时间格式

	private String ip;
	private String time;
	private String day;
	private String traffic;
	private String type;
	private String id;
	private static Date parse;

	// 重写Mapper方法
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		String[] fields = line.split(",");

		// if (fields == null || fields.length < 6) { // 有异常数据，进行消息提醒
		// System.out.println("数据异常");
		// return;
		// }

		// 对变量进行赋值封装
		Date date = parseDateFormat(fields[1]);
		ip = fields[0];
		time = dateformat1.format(date);
		day = fields[2];
		// 去掉数据中包含的空格
		traffic = fields[3].trim();
		type = fields[4];
		id = fields[5];
		// ip_sum = 1;

		// 将变量打包发送
		context.write(new Text(ip), new AccessLogWritable(ip, time, day, traffic, type, id));

	}

	// 重写setup方法，转换时间格式
	private static Date parseDateFormat(String string) {
		try {
			parse = FORMAT.parse(string);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return parse;
	}

	// // 释放资源
	// @Override
	// protected void cleanup(Context context) throws IOException,
	// InterruptedException {
	// parse = null;
	// }

}

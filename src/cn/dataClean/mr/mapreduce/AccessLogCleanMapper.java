package cn.dataClean.mr.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ��ϴ����ʱ��mapper��
 * 
 * @author Lenovo
 *
 */
public class AccessLogCleanMapper extends Mapper<LongWritable, Text, Text, AccessLogWritable> {

	// ���ñ��ļ�ʱ���ʽ��Ŀ���ļ���ʽ������ʱ���ʽ��ת��
	public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH); // ԭʱ���ʽ
	public static final SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// ����ʱ���ʽ

	private String ip;
	private String time;
	private String day;
	private String traffic;
	private String type;
	private String id;
	private static Date parse;

	// ��дMapper����
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		String[] fields = line.split(",");

		// if (fields == null || fields.length < 6) { // ���쳣���ݣ�������Ϣ����
		// System.out.println("�����쳣");
		// return;
		// }

		// �Ա������и�ֵ��װ
		Date date = parseDateFormat(fields[1]);
		ip = fields[0];
		time = dateformat1.format(date);
		day = fields[2];
		// ȥ�������а����Ŀո�
		traffic = fields[3].trim();
		type = fields[4];
		id = fields[5];
		// ip_sum = 1;

		// �������������
		context.write(new Text(ip), new AccessLogWritable(ip, time, day, traffic, type, id));

	}

	// ��дsetup������ת��ʱ���ʽ
	private static Date parseDateFormat(String string) {
		try {
			parse = FORMAT.parse(string);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return parse;
	}

	// // �ͷ���Դ
	// @Override
	// protected void cleanup(Context context) throws IOException,
	// InterruptedException {
	// parse = null;
	// }

}

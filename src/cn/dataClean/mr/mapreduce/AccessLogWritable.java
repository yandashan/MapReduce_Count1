package cn.dataClean.mr.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * ������ϴʱ�Զ����������л��ӿ�
 * 
 * @author Lenovo
 *
 */
public class AccessLogWritable implements Writable {

	private String ip;
	private String time;
	private String day;
	private String traffic;
	private String type;
	private String id;
	// private String ip_sum;

	// ����ʹ���޲ι��캯�������򸲸Ǻ���ִ��
	public AccessLogWritable() {

	}

	public AccessLogWritable(String ip, String time, String day, String traffic, String type, String id) {
		super();
		this.ip = ip;
		this.time = time;
		this.day = day;
		this.traffic = traffic;
		this.type = type;
		this.id = id;
		// this.ip_sum = ip_sum;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public String getTraffic() {
		return traffic;
	}

	public void setTraffic(String traffic) {
		this.traffic = traffic;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	// public String getIp_sum() {
	// return ip_sum;
	// }
	//
	// public void setIp_sum(String ip_sum) {
	// this.ip_sum = ip_sum;
	// }

	/**
	 * hadoopϵͳ�ڷ����л���ʱ������������
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.ip = in.readUTF();
		this.time = in.readUTF();
		this.day = in.readUTF();
		this.traffic = in.readUTF();
		this.type = in.readUTF();
		this.id = in.readUTF();
		// this.ip_sum = in.readUTF();
	}

	/**
	 * hadoopϵͳ�����л���ʱ������������
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(ip);
		out.writeUTF(time);
		out.writeUTF(day);
		out.writeUTF(traffic);
		out.writeUTF(type);
		out.writeUTF(id);
		// out.writeUTF(ip_sum);
	}

	/**
	 * ��дtoString()�������Զ��������ʽ
	 */
	@Override
	public String toString() {
		return this.ip + "," + this.time + "," + this.day + "," + this.traffic + "," + this.type + "," + this.id;
	}


}

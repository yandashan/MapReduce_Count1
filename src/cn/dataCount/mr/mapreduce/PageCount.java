package cn.dataCount.mr.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * ��װPageCount���л��ӿ�,��������ϴ��Ľ�����ж�������
 * 
 * 1����һ�����ͷ���map�е�key�д��ͱ�����Ҫ����Writable��Comparable
 * 
 * 
 * @author Lenovo
 *
 */
public class PageCount implements WritableComparable<PageCount> {
	private String page;
	private String type;
	private int count;

	public PageCount() {

	}

	public PageCount(String page, String type, int count) {
		this.page = page;
		this.type = type;
		this.count = count;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getPage() {
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.page = in.readUTF();
		this.type = in.readUTF();
		this.count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.page);
		out.writeUTF(this.type);
		out.writeInt(this.count);
	}

	// ��������
	@Override
	public int compareTo(PageCount o) {
		return o.getCount() - this.count == 0 ? this.page.compareTo(o.getPage()) : o.getCount() - this.count;
	}

	@Override
	public String toString() {
		return this.page + "\t" + this.type + "\t" + this.count;
	}

}

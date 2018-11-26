package com.hiya.da.hadoop.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HiyaEmpSalarySortMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{
	private static final Log hiyaLog = LogFactory.getLog("HiyaSumDeptSalaryMapper");

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		hiyaLog.info("HiyaSumDeptSalaryMapper>map>key=" + key);
		hiyaLog.info("HiyaSumDeptSalaryMapper>map>value=" + value);
		// 对员工文件字段进行拆分
		String[] kv = value.toString().split(",");

		// 输出key为员工所有工资和value为员工姓名
		int empAllSalary = "".equals(kv[6]) ? Integer.parseInt(kv[5]): Integer.parseInt(kv[5]) + Integer.parseInt(kv[6]);
		context.write(new IntWritable(empAllSalary), new Text(kv[1]));
	}
}

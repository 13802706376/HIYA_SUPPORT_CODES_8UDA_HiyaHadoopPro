package com.hiya.da.hadoop.reduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HiyaSumDeptSalaryReduce extends Reducer<Text, Text, Text, LongWritable>
{
	private static final Log hiyaLog =LogFactory.getLog("HiyaSumDeptSalaryReduce");
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		hiyaLog.info("HiyaSumDeptSalaryReduce>reduce>key="+key);
		hiyaLog.info("HiyaSumDeptSalaryReduce>reduce>values="+values);
		// 对同一部门的员工工资进行求和
		long sumSalary = 0;
		for (Text val : values)
		{
			sumSalary += Long.parseLong(val.toString());
		}

		// 输出key为部门名称和value为该部门员工工资总和
		context.write(key, new LongWritable(sumSalary));
	}
}

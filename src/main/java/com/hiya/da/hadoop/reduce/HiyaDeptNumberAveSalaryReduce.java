package com.hiya.da.hadoop.reduce;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HiyaDeptNumberAveSalaryReduce extends Reducer<Text, Text, Text, Text>
{
	private static final Log hiyaLog = LogFactory.getLog("HiyaSumDeptSalaryReduce");

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		hiyaLog.info("HiyaSumDeptSalaryReduce>reduce>key=" + key);
		hiyaLog.info("HiyaSumDeptSalaryReduce>reduce>values=" + values);
		long sumSalary = 0;
		int deptNumber = 0;

		// 对同一部门的员工工资进行求和
		for (Text val : values)
		{
			sumSalary += Long.parseLong(val.toString());
			deptNumber++;
		}

		// 输出key为部门名称和value为该部门员工工资平均值
		context.write(key, new Text("Dept Number:" + deptNumber + ", Ave Salary:" + sumSalary / deptNumber));
	}
}

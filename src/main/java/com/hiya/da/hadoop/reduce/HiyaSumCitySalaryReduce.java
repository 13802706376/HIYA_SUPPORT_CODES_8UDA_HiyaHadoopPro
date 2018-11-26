package com.hiya.da.hadoop.reduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HiyaSumCitySalaryReduce extends Reducer<Text, Text, Text, LongWritable>
{
	private static final Log hiyaLog = LogFactory.getLog("HiyaSumCitySalaryReduce");

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		hiyaLog.info("HiyaSumCitySalaryReduce>reduce>key=" + key);
		hiyaLog.info("HiyaSumCitySalaryReduce>reduce>values=" + values);
		// 对同一城市的员工工资进行求和
		long sumSalary = 0;
		for (Text val : values)
		{
			sumSalary += Long.parseLong(val.toString());
		}

		// 输出key为城市名称和value为该城市工资总和
		context.write(key, new LongWritable(sumSalary));
	}
}

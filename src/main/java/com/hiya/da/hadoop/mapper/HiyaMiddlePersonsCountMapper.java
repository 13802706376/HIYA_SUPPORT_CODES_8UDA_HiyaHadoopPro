package com.hiya.da.hadoop.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HiyaMiddlePersonsCountMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{
	private static final Log hiyaLog = LogFactory.getLog("HiyaMiddlePersonsCountMapper");

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		hiyaLog.info("HiyaMiddlePersonsCountMapper>map>key=" + key);
		hiyaLog.info("HiyaMiddlePersonsCountMapper>map>value=" + value);
		   // 对员工文件字段进行拆分
        String[] kv = value.toString().split(",");

        // 输出key为0和value为员工编号+","+员工经理编号
        context.write(new IntWritable(0), new Text(kv[0] + "," + ("".equals(kv[3]) ? " " : kv[3])));
	}
}

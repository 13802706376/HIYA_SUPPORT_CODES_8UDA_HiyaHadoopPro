package com.hiya.da.hadoop.mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hiya.da.hadoop.common.HiyaMapperUtils;

public class HiyaSumCitySalaryMapper extends Mapper<LongWritable, Text, Text, Text>
{
	// 用于缓存 dept文件中的数据
	private Map<String, String> deptCache = new HashMap<String, String>();
	private String[] kv;
	private static final Log hiyaLog = LogFactory.getLog("HiyaSumDeptSalaryMapper");

	// 此方法会在Map方法执行之前执行且执行一次
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		HiyaMapperUtils.setup(deptCache, context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		hiyaLog.info("HiyaSumDeptSalaryMapper>map>key=" + key);
		hiyaLog.info("HiyaSumDeptSalaryMapper>map>value=" + value);
		// 对员工文件字段进行拆分
		kv = value.toString().split(",");

		// map join: 在map阶段过滤掉不需要的数据，输出key为城市名称和value为员工工资
		if (deptCache.containsKey(kv[7]))
		{
			if (null != kv[5] && !"".equals(kv[5].toString()))
			{
				context.write(new Text(deptCache.get(kv[7].trim())), new Text(kv[5].trim()));
			}
		}
	}
}

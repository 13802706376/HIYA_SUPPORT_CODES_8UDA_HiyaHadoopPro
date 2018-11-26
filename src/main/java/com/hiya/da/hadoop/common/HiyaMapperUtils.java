package com.hiya.da.hadoop.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HiyaMapperUtils 
{
	private static final Log hiyaLog = LogFactory.getLog("HiyaMapperUtils");
	public static void setup(Map<String, String> deptCache,Context context) throws IOException, InterruptedException
	{
		BufferedReader in = null;
		try
		{
			// 从当前作业中获取要缓存的文件
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			hiyaLog.info("HiyaSumDeptSalaryMapper>setup>paths=" + paths);
			String deptIdName = null;
			for (Path path : paths)
			{
				// 对部门文件字段进行拆分并缓存到deptCache中
				if (path.toString().contains("dept"))
				{
					in = new BufferedReader(new FileReader(path.toString()));
					while (null != (deptIdName = in.readLine()))
					{
						// 对部门文件字段进行拆分并缓存到deptCache中
						deptCache.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
					}
				}
			}
			hiyaLog.info("HiyaSumDeptSalaryMapper>setup>deptCache=" + deptCache);
		} catch (IOException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (in != null)
				{
					in.close();
				}
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
}

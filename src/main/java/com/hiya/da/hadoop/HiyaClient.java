package com.hiya.da.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner; 

public class HiyaClient
{
	public static void main(String[] args) throws Exception
	{
		    String classPath = args[0];
		    Tool instance = (Tool) Class.forName(classPath).newInstance();
		    int res = ToolRunner.run(new Configuration(), instance, args);
	        System.exit(res);
	}
}

package com.hiya.da.hadoop.reduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HiyaEmpSalarySortReduce extends Reducer<Text, Text, Text, Text>
{
	private static final Log hiyaLog = LogFactory.getLog("HiyaSumDeptSalaryReduce");

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		hiyaLog.info("HiyaSumDeptSalaryReduce>reduce>key=" + key);
		hiyaLog.info("HiyaSumDeptSalaryReduce>reduce>values=" + values);
		// 员工姓名和进入公司日期
		String empName = null;
		String empEnterDate = null;

		// 设置日期转换格式和最早进入公司的员工、日期
		DateFormat df = new SimpleDateFormat("dd-MM月-yy");

		Date earliestDate = new Date();
		String earliestEmp = null;

		// 遍历该部门下所有员工，得到最早进入公司的员工信息
		for (Text val : values)
		{
			empName = val.toString().split(",")[0];
			empEnterDate = val.toString().split(",")[1].toString().trim();
			try
			{
				System.out.println(df.parse(empEnterDate));
				if (df.parse(empEnterDate).compareTo(earliestDate) < 0)
				{
					earliestDate = df.parse(empEnterDate);
					earliestEmp = empName;
				}
			} catch (ParseException e)
			{
				e.printStackTrace();
			}
		}

		// 输出key为部门名称和value为该部门最早进入公司员工
		context.write(key, new Text("The earliest emp of dept:" + earliestEmp + ", Enter             date:"
				+ new SimpleDateFormat("yyyy-MM-dd").format(earliestDate)));
	}
}

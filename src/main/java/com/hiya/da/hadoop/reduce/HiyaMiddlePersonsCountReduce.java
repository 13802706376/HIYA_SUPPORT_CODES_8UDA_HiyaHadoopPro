package com.hiya.da.hadoop.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HiyaMiddlePersonsCountReduce extends Reducer<IntWritable, Text, NullWritable, Text>
{
	// 定义员工列表和员工对应经理Map
	List<String> employeeList = new ArrayList<String>();
	Map<String, String> employeeToManagerMap = new HashMap<String, String>();

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		// 在reduce阶段把所有员工放到员工列表和员工对应经理Map中
		for (Text value : values)
		{
			employeeList.add(value.toString().split(",")[0].trim());
			employeeToManagerMap.put(value.toString().split(",")[0].trim(), value.toString().split(",")[1].trim());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		int totalEmployee = employeeList.size();
		int i, j;
		int distance;
		System.out.println(employeeList);
		System.out.println(employeeToManagerMap);

		// 对任意两个员工计算出沟通的路径长度并输出
		for (i = 0; i < (totalEmployee - 1); i++)
		{
			for (j = (i + 1); j < totalEmployee; j++)
			{
				distance = calculateDistance(i, j);
				String value = employeeList.get(i) + " and " + employeeList.get(j) + " =   "+ distance;
				context.write(NullWritable.get(), new Text(value));
			}
		}
	}

	/**
	 * 该公司可以由所有员工形成树形结构，求两个员工的沟通的中间节点数，可以转换在员工树中两员工之间的距离
	 * 由于在树中任意两点都会在某上级节点汇合，根据该情况设计了如下算法
	 */
	private int calculateDistance(int i, int j)
	{
		String employeeA = employeeList.get(i);
		String employeeB = employeeList.get(j);
		int distance = 0;

		// 如果A是B的经理，反之亦然
		if (employeeToManagerMap.get(employeeA).equals(employeeB)
				|| employeeToManagerMap.get(employeeB).equals(employeeA))
		{
			distance = 0;
		}
		// A和B在同一经理下
		else if (employeeToManagerMap.get(employeeA).equals(employeeToManagerMap.get(employeeB)))
		{
			distance = 0;
		} else
		{
			// 定义A和B对应经理链表
			List<String> employeeA_ManagerList = new ArrayList<String>();
			List<String> employeeB_ManagerList = new ArrayList<String>();

			// 获取从A开始经理链表
			employeeA_ManagerList.add(employeeA);
			String current = employeeA;
			while (false == employeeToManagerMap.get(current).isEmpty())
			{
				current = employeeToManagerMap.get(current);
				employeeA_ManagerList.add(current);
			}

			// 获取从B开始经理链表
			employeeB_ManagerList.add(employeeB);
			current = employeeB;
			while (false == employeeToManagerMap.get(current).isEmpty())
			{
				current = employeeToManagerMap.get(current);
				employeeB_ManagerList.add(current);
			}

			int ii = 0, jj = 0;
			String currentA_manager, currentB_manager;
			boolean found = false;

			// 遍历A与B开始经理链表，找出汇合点计算
			for (ii = 0; ii < employeeA_ManagerList.size(); ii++)
			{
				currentA_manager = employeeA_ManagerList.get(ii);
				for (jj = 0; jj < employeeB_ManagerList.size(); jj++)
				{
					currentB_manager = employeeB_ManagerList.get(jj);
					if (currentA_manager.equals(currentB_manager))
					{
						found = true;
						break;
					}
				}

				if (found)
				{
					break;
				}
			}

			// 最后获取两只之前的路径
			distance = ii + jj - 1;
		}
		return distance;
	}
}

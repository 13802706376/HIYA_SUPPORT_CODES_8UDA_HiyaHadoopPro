package com.hiya.da.hadoop.runner;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.hiya.da.hadoop.HiyaClient;
import com.hiya.da.hadoop.mapper.HiyaSumDeptSalaryMapper;
import com.hiya.da.hadoop.reduce.HiyaSumDeptSalaryReduce;

/**
 * 求各个部门的总工资 
 * MapReduce中的join分为好几种，比如有最常见的 reduce side join、map side join和semi join 等。reduce join 在shuffle阶段要进行大量的数据传输，会造成大量的网络IO效率低下，而map side join 在处理多个小表关联大表时非常有用 。
Map side join是针对以下场景进行的优化：两个待连接表中，有一个表非常大，而另一个表非常小，以至于小表可以直接存放到内存中。这样我们可以将小表复制多份，让每个map task内存中存在一份（比如存放到hash table中），然后只扫描大表：对于大表中的每一条记录key/value，在hash table中查找是否有相同的key的记录，如果有，则连接后输出即可。为了支持文件的复制，Hadoop提供了一个类DistributedCache，使用该类的方法如下：
（1）用户使用静态方法DistributedCache.addCacheFile()指定要复制的文件，它的参数是文件的URI（如果是HDFS上的文件，可以这样：hdfs://jobtracker:50030/home/XXX/file）。JobTracker在作业启动之前会获取这个URI列表，并将相应的文件拷贝到各个TaskTracker的本地磁盘上。
（2）用户使用DistributedCache.getLocalCacheFiles()方法获取文件目录，并使用标准的文件读写API读取相应的文件。
在下面代码中，将会把数据量小的表(部门dept）缓存在内存中，在Mapper阶段对员工部门编号映射成部门名称，该名称作为key输出到Reduce中，在Reduce中计算按照部门计算各个部门的总工资。
 
 *  运行步骤
 *  （1）cd /home/dev/ops/local/hadoop-2.8.4/programes
 *  （2）hadoop jar /home/dev/ops/local/hadoop-2.8.4/programes/HiyaHadoopPro-0.0.1-SNAPSHOT.jar com.hiya.da.hadoop.HiyaClient hdfs://sz-test-a-ditui-erp.novalocal:9000/dept hdfs://sz-test-a-ditui-erp.novalocal:9000/emp /home/dev/ops/local/hadoop-2.8.4/programes/out/output
 *
 */
public class HiyaSumDeptSalaryRunner extends Configured implements Tool
{
	private static final Log hiyaLog =LogFactory.getLog("HiyaSumDeptSalaryReduce");
	
	@Override
	public int run(String[] arg0) throws Exception
	{
		hiyaLog.info("HiyaSumDeptSalaryRunner  start.....");
		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf(), "HiyaSumDeptSalary");
		job.setJobName("HiyaSumDeptSalary");
		job.setJarByClass(HiyaClient.class);
		job.setMapperClass(HiyaSumDeptSalaryMapper.class);
		job.setReducerClass(HiyaSumDeptSalaryReduce.class);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);

		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), arg0).getRemainingArgs();
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
		hiyaLog.info("HiyaSumDeptSalaryRunner  end.....");
		return job.isSuccessful() ? 0 : 1;
	}
}

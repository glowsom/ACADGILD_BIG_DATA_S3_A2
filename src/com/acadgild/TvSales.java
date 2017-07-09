package com.acadgild;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TvSales {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		/*
		 *  Entry point of program which will call TvSalesMapper class
		 *  and every other class needed for the job to be done.
		 */
		Configuration conf = new Configuration();	//Get configuration details
		Job job =new Job(conf, "TvSales");	//Job object is created
		job.setJarByClass(TvSales.class);	//Set main class

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(TvSalesMapper.class);	//Set mapper class
		job.setNumReduceTasks(0);	//Set number of reducers to 0
		
		//Because there isn't a reducer, the framework will handle it
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * Input and Output Paths will be provided in command line.
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}

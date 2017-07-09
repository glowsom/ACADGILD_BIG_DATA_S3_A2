package com.acadgild;
/*
 * File television.txt has some records which contain "NA"
 * in either Company or Product columns. 
 * Filter out all such records from file.
 * 
 * My code wasn't executing properly with "|" separator.
 * After showing my work to instructor, he mentioned that
 * MapReduce is know to have some problems with it, and told me
 * to replace that separator with "," 
 */
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class TvSalesMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {		
		/*
		 * For every record, fields are separated by ","
		 * This will assign each field to the corresponding
		 * array index for easy parsing.
		 */
		String[] line = value.toString().split(",");
		/*
		 * Record has this format:
		 * Company Name|Product Name|Size in inches|State|Pin Code|Price
		 * 
		 * For a record to be invalid, either Company Name field or 
		 * Product Name field must contain this String "NA".
		 * The array indices 0 and 1 represent these fields.
		 * 
		 * The map function will skip this record if either of these fields
		 * contain "NA"
		 */
		if (!(line[0].equalsIgnoreCase("NA") || line[1].equalsIgnoreCase("NA"))){
			//Omit this record on context.write
			context.write(null, value);
		}
	}
	
}
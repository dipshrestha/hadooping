package com.dipesh.hadoop.wordcount.v2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool{
	private final static Logger logger = Logger.getLogger(WordCount.class);
	
	public static void main(String[] args) throws Exception{
		logger.info("Starting...");
		int exitCode = ToolRunner.run(new WordCount(), args);
		logger.info("Exiting with code:" + exitCode);
		System.exit(exitCode);
	}
 
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			logger.error(String.format("Usage: %s <inputPath> <outputPath>\n", getClass().getCanonicalName()));
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
	
		// job configurations
		Job job = new org.apache.hadoop.mapreduce.Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCounter");
	
		// executor configurations
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		// input/output formats configurations
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		// input/output file configurations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int returnValue = job.waitForCompletion(true) ? 0:1;
		logger.info("job.isSuccessful " + job.isSuccessful());
		return returnValue;
	}
}
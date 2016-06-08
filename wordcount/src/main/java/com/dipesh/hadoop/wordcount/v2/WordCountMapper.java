package com.dipesh.hadoop.wordcount.v2;

import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger; 

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private final static Logger logger = Logger.getLogger(WordCountMapper.class);
	private final static IntWritable ONE = new IntWritable(1);
	private final static String DELIMITER = " ";
    private Text word = new Text();
    
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		logger.debug(String.format("(%s, %s)", key.toString(), value.toString()));
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, DELIMITER);
		while(st.hasMoreTokens()){
			word.set(st.nextToken());
			context.write(word, ONE);
		}
	}
}
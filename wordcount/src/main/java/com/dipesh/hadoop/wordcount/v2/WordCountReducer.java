package com.dipesh.hadoop.wordcount.v2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private final static Logger logger = Logger.getLogger(WordCountReducer.class);
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	
		int sum = 0, temp = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		while(valuesIt.hasNext()){
			temp = valuesIt.next().get();
			sum += temp;
			logger.debug(String.format("(%s, %d)", key.toString(), temp));
		}
		logger.debug(String.format("Final (%s, %d)", key.toString(), sum));
		context.write(key, new IntWritable(sum));
	}	
}
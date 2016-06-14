package com.dipesh.hadoop.wordcount.v2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapperTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void setUp() {
	    WordCountMapper mapper = new WordCountMapper();
	    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable> (mapper);
	}
	
	@Test
	public void testMap() {
		mapDriver.withInput(new LongWritable(), new Text("Hello abc Hello World"));
		
		mapDriver.withOutput(new Text("Hello"), new IntWritable(1))
			.withOutput(new Text("abc"), new IntWritable(1))
			.withOutput(new Text("Hello"), new IntWritable(1))
			.withOutput(new Text("World"), new IntWritable(1));
		
		mapDriver.runTest();
	}
}

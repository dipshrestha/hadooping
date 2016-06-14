package com.dipesh.hadoop.wordcount.v2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountReducerTest {

	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	
	@Before
	public void setUp() {
	    WordCountReducer reducer = new WordCountReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable> (reducer);
	}
	
	@Test
	public void testReduce() {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		Text key = new Text("Hello");
		
		IntWritable expectedValue = new IntWritable(3);
		
		reduceDriver.withInput(key, values).withOutput(key, expectedValue);
		reduceDriver.runTest();
	}
}

package com.talis.mapreduce.wordcount.oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class WordCountReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>  {

	private final static Logger LOG = LoggerFactory.getLogger(WordCountReducer.class); 
	private IntWritable result = new IntWritable();

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			int v = values.next().get();
			sum += v;
			LOG.info("< " + key + " " + v);
		}
		result.set(sum);
		LOG.info("> " + key + " " + result);
		output.collect(key, result);
	}

}
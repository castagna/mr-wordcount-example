package com.talis.mapreduce.wordcount.oldapi;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private final static Logger LOG = LoggerFactory.getLogger(WordCountMapper.class); 
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		LOG.info("< " + key + " " + value);
		StringTokenizer iter = new StringTokenizer(value.toString(), " \t\n\r\f.,;:!?", false);
		while (iter.hasMoreTokens()) {
			word.set(iter.nextToken().toLowerCase());
			LOG.info("> " + word + " " + one);
			output.collect(word, one);
		}
	}

}
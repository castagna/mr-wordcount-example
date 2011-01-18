/*
 * Copyright © 2011 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.talis.mapreduce.wordcount.newapi;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException 
	{
		StringTokenizer iter = new StringTokenizer(value.toString(), " \t\n\r\f.,;:!?", false);
		while (iter.hasMoreTokens()) 
		{
			word.set(iter.nextToken().toLowerCase());
			context.write(word, one);
		}
    }

}
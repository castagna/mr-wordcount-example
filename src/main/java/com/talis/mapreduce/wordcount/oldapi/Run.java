package com.talis.mapreduce.wordcount.oldapi;

public class Run {

	public static void main(String[] args) throws Exception {
		WordCount.main(new String[] { "src/test/resources/loremipsum2", "target/output_" + System.currentTimeMillis() } );
	}

}

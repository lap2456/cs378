package com.refactorlabs.cs378.assign5;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Map class for word statistics with AVRO.  Extends class Mapper, provided by Hadoop.
 * This class defines the map() function for the word statistics and uses AVRO and the 
 * WordStatisticsData class
 * 
 * @author Louis Pujol (louispujol@yahoo.com)
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

	/**
	 * Each count output from the map() function is "1", so to minimize small
	 * object creation we can use a constant for this output value/object.
	 */
	public final static long ONE = 1L;

	/**
	 * Local variable "word" will contain the word identified in the input.
	 * The Hadoop Text object is mutable, so we can reuse the same object and
	 * simply reset its value as each word in the input is encountered.
	 */
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//Convert our text to string
		String line = value.toString();

		//Create tokenizer for input
		StringTokenizer tokenizer = new StringTokenizer(line);

		//Instantiate a map that will tally up the frequencies of each word
		Map<String, Long> wordMap = new TreeMap<String, Long>();


		// For each tokenized word in the input line, 
		// store the word in a map with its new frequency
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();

			String[] tokens = cleanUpToken(token);
			for(String st: tokens){
				if (!wordMap.containsKey(st)) //Create new k-v pairing
					wordMap.put(st, (long)ONE);
				else
					wordMap.put(st, wordMap.get(st) + ONE); //Use existing k-v pairing
			}
		}

		//Iterate each key in the map (key = word)
		for (String keys : wordMap.keySet()) {
			word.set(keys);

			//Create builder to save our values from wordmap
			WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
			
			//store our longs into builder
			builder.setDocumentCount(ONE);
			builder.setTotalCount(wordMap.get(keys));
			builder.setSumOfSquares((wordMap.get(keys) * wordMap.get(keys)));
			builder.setMean((double)0);
			builder.setVariance((double)0);
			
		
			//Write output to context as text and avrovalue
			context.write( word, new AvroValue(builder.build()));
		}
	}

	/*
	* Will examine given token for punctuation or if the token should be broken up
	* It will return a list of strings where the size depends on whether the token
	* needed to be broken apart and the strings will all be lowercase
	* ex) '"Hello[1234]' should be 'hello' and '[1234]'
	* @param token: the given token that will be cleaned
	*/
	public static String[] cleanUpToken(String token){
		token = token.replaceAll("[^a-z'A-Z0-9\\[\\]]", "").toLowerCase();
		token = token.replaceAll("[\\[]", " [");
		token = token.replaceAll("[\\]]", "] ");
		String[] tokens = token.trim().split(" ");
		return tokens;
	}
}
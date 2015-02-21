package com.refactorlabs.cs378.assign2;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
 * This class defines the map() function for the word statistics
 * 
 * @author Louis Pujol (louispujol@yahoo.com)
 * @author David Franke (dfranke@cs.utexas.edu)
 */

public class MapClass extends Mapper<LongWritable, Text, Text, DoubleArrayWritable> {
	
	//Instantiate Text object that will be used for writing to context
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//Convert our text to string
		String line = value.toString();
		
		//Create tokenizer for input
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		//Instantiate a map that will tally up the frequencies of each word
		Map<String, Double> wordMap = new TreeMap<String, Double>();
		
		
		// For each tokenized word in the input line, 
		// store the word in a map with its new frequency
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			String[] tokens = cleanUpToken(token);
			for(String st: tokens){
				if (!wordMap.containsKey(st)) //Create new k-v pairing
					wordMap.put(st, 1.);
				else
					wordMap.put(st, wordMap.get(st) + 1); //Use existing k-v pairing
			}		
		}

		//Iterate each key in the map (key = word)
		for (String keys : wordMap.keySet()) {
			word.set(keys);
			//Store each value in the writable array
			Writable[] writable = new Writable[3];
			//Writable[0] represents DOCUMENT_COUNT
			writable[0] = new DoubleWritable(1D); 
			//Writable[1] represents FREQUENCY
			writable[1] = new DoubleWritable(wordMap.get(keys));
			//Writable[2] represents FREQUENCY_SQUARED
			writable[2] = new DoubleWritable(wordMap.get(keys) * wordMap.get(keys));
			//Store writable array in a DoubleArrayWritable object
			DoubleArrayWritable dw = new DoubleArrayWritable();
			dw.set(writable);
			//Write to context
			context.write( word, dw);
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
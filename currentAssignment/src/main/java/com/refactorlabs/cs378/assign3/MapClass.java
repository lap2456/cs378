package com.refactorlabs.cs378.assign3;

import java.io.IOException;
import java.lang.StringBuilder;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Map class for Inverted Index.  Extends class Mapper, provided by Hadoop.
 * This class defines the map() function for the inverted index
 * 
 * @author Louis Pujol (louispujol@yahoo.com)
 * @author David Franke (dfranke@cs.utexas.edu)
 */

public class MapClass extends Mapper<LongWritable, Text, Text, Text> {

	//Instantiate Text object that will be used for writing to context
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
			{

		//Convert our text to string
		String line = value.toString();

		//Create tokenizer for input
		StringTokenizer tokenizer = new StringTokenizer(line);

		//will be a map from word in a verse to all of the references of that word 
		Map<String, HashSet<String>> wordToReference = new TreeMap<String, HashSet<String>>();

		String currentBookAndVerse = "";
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();

			//Determine if we have found a book:chapter:verse
			if (token.matches("[a-zA-Z]+:[0-9]?[0-9]:[0-9]?[0-9]")){
				currentBookAndVerse = token;
			}

			//Otherwise clean token up
			else{
				token = cleanUpToken(token);
				HashSet<String> references;
				//if we have seen this word before then add grab the existing set
				if(wordToReference.containsKey(token)){
					references = wordToReference.get(token);
				}
				//otherwise create a new one
				else{
					references = new HashSet<String>();
				}
				//add the word and put the set back in the map
				references.add(currentBookAndVerse);
				wordToReference.put(token, references);
			}
		}

		//Iterate each key in the map (key = word)
		for (String keys : wordToReference.keySet()) {
			word.set(keys);
			Set<String> references = wordToReference.get(keys);
			String outputString = join(references, " ");
			Text output = new Text();
			output.set(outputString);
			
			//Write to context
			context.write( word, output);
		}

	}
   /*
	* Will examine given token for punctuation and remove if necessary
	* 
	* @param token: the given token that will be cleaned
	*/
	public static String cleanUpToken(String token){
		return token.replaceAll("[^a-z'A-Z0-9]", "").toLowerCase();
	}

   /*
	* Will join a Set of strings together with a seperator
	* and then will delete the trailing seperator
	* 
	* @param token: the given token that will be cleaned
	*/
	public static String join(Set<String> words, String seperator){
		//using a stringbuilder for time efficiency
		StringBuilder result = new StringBuilder();
		for(String word : words){
			if(word.length() > 0)
				result.append(word + seperator);
		}
		//find the last seperator (ex. ',' or ' ') and remove it so that there is no trailing one
		if(result.length() > 0)
			result.delete(result.lastIndexOf(seperator), result.length());

		return result.toString();
	}

}
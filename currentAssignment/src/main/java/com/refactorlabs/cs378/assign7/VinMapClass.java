package com.refactorlabs.cs378.assign7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Louis Pujol (louispujol@yahoo.com)
 * @author David Franke (dfranke@cs.utexas.edu)
 * 
 * Mapper for UserSessions. Extends class Mapper, provided by Hadoop.
 * Mapper will take User sessions and output session objects
 * as values and the user id and apikey as the key. 
 */
public class VinMapClass extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		
		VinImpressionCounts.Builder vinBuilder = new VinImpressionCounts.newBuilder();
		
		String[] values = line.split(",");
		Long unique_users = Long.parseLong(values[2]);

		vinBuilder.setUniqueUserVdpView(unique_users);
		word.set(values[0]);

		//Write key and session (wrapped in avro value) to context
		context.write( word, new AvroValue<VinImpressionCounts>(vinBuilder.build()));
	}
}
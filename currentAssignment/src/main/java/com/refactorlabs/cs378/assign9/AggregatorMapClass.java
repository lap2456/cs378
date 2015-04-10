package com.refactorlabs.cs378.assign9;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
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

public class AggregatorMapClass extends Mapper<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>, Text, AvroValue<ClickSubtypeStatisticsData>> {

	private Text wordSession = new Text();
	private Text wordAny = new Text();

	@Override
	public void map(AvroKey<ClickSubtypeStatisticsKey> key, AvroValue<ClickSubtypeStatisticsData> value, Context context)
			throws IOException, InterruptedException {
		ClickSubtypeStatisticsKey keyData = key.datum();
		CharSequence clickSubtype = keyData.getClickSubtype();
		ClickSubtypeStatisticsKey.Builder allData = ClickSubtypeStatisticsKey.newBuilder();
		allData.setClickSubtype(clickSubtype);
		allData.setSessionType("any");
		wordSession.set(keyData.toString());
		wordAny.set((allData.build()).toString());

		context.write(wordSession, value);
		context.write(wordAny, value);
	}
}
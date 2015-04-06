package com.refactorlabs.cs378.assign9;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

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

public class SubmitterMapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<clickSubtypeStatsKey>, AvroValue<clickSubtypeStatsData>> {
	
	//Instantiate Text object that will be used for writing to context
	private Text word = new Text();

	@Override
	public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
			throws IOException, InterruptedException {

		CharSequence keyDatum = key.datum();
		List<Event> events = value.datum().getEvents();
		
		Map<EventSubtype, clickSubtypeStatsData.Builder> subtypeMap = new TreeMap<EventSubtype, clickSubtypeStatsData.Builder>();
		Long totalEvents = 0L;
		for(Event e : events){
			clickSubtypeStatsData.Builder thisBuilder;
			thisBuilder = subtypeMap.get(e.getEventSubtype());
			if(thisBuilder == null){
				thisBuilder = new clickSubtypeStatsData.newBuilder();
			}

			if(thisBuilder.hasSessionCount()){
				thisBuilder.setSessionCount(thisBuilder.getSessionCount() + 1L);
			}
			else{
				thisBuilder.setSessionCount(1L);
			}
			totalEvents += 1L;
		}

		for(clickSubtypeStatsData.Builder thisBuilder : subtypeMap.keySet()){
			thisBuilder.setTotalCount(totalEvents);
			thisBuilder.set
		}
	}
}
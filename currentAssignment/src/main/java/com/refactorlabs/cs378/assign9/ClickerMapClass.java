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

public class ClickerMapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {

	@Override
	public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
			throws IOException, InterruptedException {

		CharSequence keyDatum = key.datum();
		List<Event> events = value.datum().getEvents();
		
		Map<EventSubtype, ClickSubtypeStatisticsData.Builder> subtypeMap = new TreeMap<EventSubtype, ClickSubtypeStatisticsData.Builder>();
		Long totalEvents = 0L;
		for(Event e : events){
			ClickSubtypeStatisticsData.Builder thisBuilder;
			thisBuilder = subtypeMap.get(e.getEventSubtype());
			if(thisBuilder == null){
				thisBuilder = new ClickSubtypeStatisticsData.newBuilder();
			}

			if(thisBuilder.hasSessionCount()){
				thisBuilder.setSessionCount(thisBuilder.getSessionCount() + 1L);
			}
			else{
				thisBuilder.setSessionCount(1L);
			}
			totalEvents += 1L;
		}

		for(EventSubtype thisSubtype : subtypeMap.keySet()){
			ClickSubtypeStatisticsData.Builder thisBuilder = subtypeMap.get(thisSubtype);
			thisBuilder.setTotalCount(totalEvents);
			
			ClickSubtypeStatisticsKey.Builder keyBuilder = ClickSubtypeStatisticsKey.newBuilder();
			keyBuilder.setSessionType("CLICK");
			keyBuilder.setClickSubtype(convertSubtypeToString(thisSubtype));
			context.write(new AvroKey<ClickSubtypeStatisticsKey>(keyBuilder.build()), new AvroValue<ClickSubtypeStatisticsData>(thisBuilder.build()));
		}
	}

	private String convertSubtypeToString(EventSubtype eS){
		switch(eS){
			case CONTACT_FORM_TYPE : return "CONTACT_FORM_TYPE";
			case ALTERNATIVE : return "ALTERNATIVE";
			case CONTACT_BANNER : return "CONTACT_BANNER";
			case CONTACT_BUTTON : return "CONTACT_BUTTON";
			case DEALER_PHONE : return "DEALER_PHONE";
			case FEATURES : return "FEATURES";
			case GET_DIRECTIONS : return "GET_DIRECTIONS";
			case SHOW_MORE_BADGES : return "SHOW_MORE_BADGES";
			case TEST_DRIVE_LINK : return "TEST_DRIVE_LINK";
			case VEHICLE_HISTORY : return "VEHICLE_HISTORY";
			case FORM_ERROR : return "FORM_ERROR";
			case FORM_SUCCESS : return "FORM_SUCCESS";
			case CONTACT_FORM : return "CONTACT_FORM";
			case MARKET_REPORT : return "MARKET_REPORT";
			case BADGE_DETAIL : return "BADGE_DETAIL";
			case PHOTO_MODAL : return "PHOTO_MODAL";
			case BADGES : return "BADGES";
		}
	}
}
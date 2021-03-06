package com.refactorlabs.cs378.assign7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
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
public class SessionMapClass extends Mapper<AvroKey<CharSequence>, AvroValue<com.refactorlabs.cs378.assign7.Session>, Text, AvroValue<VinImpressionCounts>> {

	@Override
	public void map(AvroKey<CharSequence> key, AvroValue<com.refactorlabs.cs378.assign7.Session> value, Context context)
			throws IOException, InterruptedException {

		CharSequence keyDatum = key.datum();
		List<Event> events = value.datum().getEvents();
		HashMap<String, VinImpressionCounts.Builder> vinToCounts = new HashMap<String, VinImpressionCounts.Builder>();
		for(Event event : events){
			String vin = String.valueOf(event.getVin());
			VinImpressionCounts.Builder vinBuilder;
			if(vinToCounts.containsKey(vin)){
				vinBuilder = vinToCounts.get(vin);
			}
			else{
				vinBuilder = VinImpressionCounts.newBuilder();
			}
			vinBuilder.setUniqueUser(vinBuilder.getUniqueUser() + 1L);
			if(event.getEventType() == EventType.SHARE && event.getEventSubtype() == EventSubtype.MARKET_REPORT){
				vinBuilder.setShareMarketReport(vinBuilder.getShareMarketReport() + 1);
			}
			else if(event.getEventType() == EventType.SUBMIT && event.getEventSubtype() == EventSubtype.CONTACT_FORM){
				vinBuilder.setSubmitContactForm(vinBuilder.getSubmitContactForm() + 1);
			}
			else if(event.getEventType() == EventType.CLICK){
				Map<CharSequence, Long> userToNum; 
				if(vinBuilder.hasClicks()){
					userToNum = vinBuilder.getClicks();
				}
				else{
					userToNum = new HashMap<CharSequence, Long>();
				}
				if(userToNum.containsKey(keyDatum)){
					userToNum.put(keyDatum, userToNum.get(keyDatum) + 1L);
				}
				else{
					userToNum.put(keyDatum, 1L);
				}
				vinBuilder.setClicks(userToNum);
			}
			vinToCounts.put(vin, vinBuilder);
		}

		for(String vin : vinToCounts.keySet()){
			VinImpressionCounts.Builder thisVinImpression = vinToCounts.get(vin);
			context.write(new Text(vin), new AvroValue<VinImpressionCounts>(thisVinImpression.build()));
		}

	}
}
package com.refactorlabs.cs378.assign9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
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
public class PreRunMapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>>{
	
	private AvroMultipleOutputs multipleOutputs;
	private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

	@Override
	public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
			throws IOException, InterruptedException {

		CharSequence keyDatum = key.datum();
		List<Event> events = value.datum().getEvents();
		
		Boolean isSubmitter = false;
		Boolean isSharer = false;
		Boolean isClicker = false;
		Boolean isShower = false;
		Boolean isVisitor = false;
		Boolean isOther = true;

		for(Event event : events){
			EventType thisType = event.getEventType();
			if(thisType == EventType.CHANGE || thisType == EventType.CONTACT_FORM_STATUS || thisType == EventType.EDIT || thisType == EventType.SUBMIT){
				isSubmitter = true;
				break;
			}
			else if(thisType == EventType.SHARE){
				isSharer = true;
			}
			else if(thisType == EventType.CLICK){
				isClicker = true;
			}
			else if(thisType == EventType.SHOW){
				isShower = true;
			}
			else if(thisType == EventType.VISIT){
				isVisitor = true;
			}
		}

		String sessionCategory;
		if(isSubmitter){
			sessionCategory = "Submitter";
		}
		else if(isSharer){
			sessionCategory = "Sharer";
		} 
		else if(isClicker){
			sessionCategory = "Clicker";
		}
		else if(isShower){
			sessionCategory = "Shower";
		}
		else if(isVisitor){
			sessionCategory = "Visitor";
		}
		else{
			sessionCategory = "Other";
		}

		multipleOutputs.write( sessionCategory, key, value);
		context.getCounter(MAPPER_COUNTER_GROUP, sessionCategory).increment(1L);
		
	}

	//Setup method to initialzie multiple outputs
	@Override
	public void setup(Context context) {
		multipleOutputs = new AvroMultipleOutputs(context);
	} 

	//Cleanup method to close multiple outputs
	@Override
	public void cleanup(Context context) throws InterruptedException,IOException {
		multipleOutputs.close();
	}
}
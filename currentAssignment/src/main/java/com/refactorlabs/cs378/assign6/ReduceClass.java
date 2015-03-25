package com.refactorlabs.cs378.assign6;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reduce class for UserSessions.  Extends class Reducer, provided by Hadoop.
 * This class defines the reduce() function for the UserSessions example. Reducer
 * will take sessions with the same key and combine their Events into a 
 * single session for output.
 * 
 * @author Louis Pujol (louispujol@yahoo.com)
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class ReduceClass
extends Reducer<Text, AvroValue<Session>,
AvroKey<Pair<CharSequence, Session>>, NullWritable> {


	@Override
	public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
			throws IOException, InterruptedException {
		//finalSession stores our output session
		Session.Builder finalSession = Session.newBuilder();

		//eventist will store unsorted Events
		List<Event> eventist = new ArrayList<Event>();

		//Flag that keeps track of first iteration
		boolean firstIteration = true;

		//Iterate through all sessions and take the Events out to store 
		//into finalSession
		for (AvroValue<Session> value : values) {

			//Save all of our session data using the first session object
			if (firstIteration){
				finalSession.setUserId(value.datum().getUserId());
				firstIteration = false;
			}

			//Take the Event data from our session and store in tempEventList
			List<Event> tempEventList = value.datum().getEvents();
			for (int i = 0 ; i < tempEventList.size() ; i++){
				//Take each Event and create a copy
				Event newEvent = Event.newBuilder(tempEventList.get(i)).build();
				//Store the copied Event in eventist
				eventist.add(newEvent);
			}
		}

		//Create our finalEventList using a sorted list of the Events in eventist
		List<Event> finalEventList = getSortedEventList(eventist);

		//Store the Events in our final session
		finalSession.setEvents(finalEventList);

		//Write to context using avroKey with key and final session
		if(finalEventList.size() == 50)
			context.write(
				new AvroKey<Pair<CharSequence, Session>>
				(new Pair<CharSequence, Session>(key.toString(), finalSession.build())),
				NullWritable.get());
	}


	//This method takes an unsorted list of Events and sorts them based on their
	//timestamp information
	private List<Event> getSortedEventList(List<Event> eventist) {
		//Create a list of ComparableEvent which wrap
		//Event objects with comparable interface
		List<ComparableEvent> sortedEventList = new ArrayList<ComparableEvent>();
		for (int i = 0 ; i < eventist.size() ; i++){
			ComparableEvent sortedEvent = new ComparableEvent(eventist.get(i));
			sortedEventList.add(sortedEvent);
		}

		//Sort Events by timestamp using ComparableEvent class
		Collections.sort(sortedEventList);

		//Unwrap ComparableEvent and store in finalEventList
		List<Event> finalEventList = new ArrayList<Event>();
		for (int i = 0 ; i < sortedEventList.size() ; i++){
			finalEventList.add(i, sortedEventList.get(i).getMyEvent());
		}

		//Return sorted Event list
		return finalEventList;
	}
}
package com.refactorlabs.cs378.assign6;

import java.lang.String;

/*
 * @author Louis Pujol
 * 
 * ComparableEvent class stores an event object and implements the comparable
 * interface. CompareTo function uses the events TimeStamp information
 * to determine the sort order for events in the session object.
 */
public class ComparableEvent implements Comparable<ComparableEvent> {

	//Private event for comparator
	private Event myEvent; 


	//Constructor sets private event
	public ComparableEvent(Event newEvent){
		setMyEvent(newEvent);
	}

	//Getter for event
	public Event getMyEvent() {
		return myEvent;
	}

	//Setter for event
	public void setMyEvent(Event myEvent) {
		this.myEvent = myEvent;
	}

	//Comparator for ComparableEvent object - checks argument 
	//against internal event
	@Override
	public int compareTo(ComparableEvent o) {
		ComparableEvent event = (ComparableEvent) o;
		String[] first = myEvent.getEventTime().split(" ");
		String[] second = event.getMyEvent().getEventTime().split(" ");
		if(first[0].compareTo(second[0]) == -1){
			return -1;
		}
		else if(first[0].compareTo(second[0]) == 1)){
			return 1;
		}
		else{
			return first[1].compareTo(second[1]);
		}
	}
}
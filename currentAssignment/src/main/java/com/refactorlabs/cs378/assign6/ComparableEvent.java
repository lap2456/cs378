package com.refactorlabs.cs378.assign6;

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

		//If our stored event has a larger timestamp than the argument, return 1
		//else, return -1
		if (myEvent.getEventTime() > event.getMyEvent().getEventTime())
			return 1;
		else if (myEvent.getEventTime() < event.getMyEvent().getEventTime())
			return -1;
		else 
			return 0;

	}
}
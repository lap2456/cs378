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
		String myTimeStamp = myEvent.getEventTime().toString();
		String theirTimeStamp = event.getMyEvent().getEventTime().toString();
		
		String firstDate = myTimeStamp.substring(0, myTimeStamp.indexOf(" "));
		String secondDate = theirTimeStamp.substring(0, theirTimeStamp.indexOf(" "));
		String firstTime = myTimeStamp.substring(firstDate.length(), myTimeStamp.length());
		String secondTime = theirTimeStamp.substring(secondDate.length(), theirTimeStamp.length());

		if(firstDate.compareTo(secondDate) < 0){
			return -1;
		}
		else if(firstDate.compareTo(secondDate) > 0){
			return 1;
		}
		else{
			return firstTime.compareTo(secondTime);
		}
	}
}
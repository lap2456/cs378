package com.refactorlabs.cs378.assign6;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
public class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {


	/**
	 * Local variable "word" will contain the key for our output
	 * The key is simply a concatenation of the apikey and userid
	 * 	 
	 * */
	private Text word = new Text();


	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//Convert our text to string
		String line = value.toString();

		//Create our session and event builders
		Session.Builder sessionBuilder = Session.newBuilder();
		Event.Builder eventBuilder = Event.newBuilder();

		//////////////////////////////////////////////////////////
		//Initialize all of our avro fields
		String eventType = getFieldValue("|type:", line).toUpperCase();
		String address = getFieldValue("address:", line);
		String city = getFieldValue("city:", line);
		String zip = getFieldValue("zip:", line);
		String state = getFieldValue("state:", line);
		String lat = getFieldValue("lat:", line);
		String total = getFieldValue("total:", line);
		String startIndex = getFieldValue("start_index:", line);
		String idnumbers = getFieldValue("|id:", line);
		idnumbers = idnumbers.replaceAll(",", " ");
		String ab = getFieldValue("ab:", line);
		String actionName = getFieldValue("action_name:", line).toUpperCase();
		String resolution = getFieldValue("res:", line);
		String uagent = getFieldValue("uagent:", line);
		String apikey = getFieldValue("apikey:", line);
		String uid= getFieldValue("uid:", line);
		String domain = getFieldValue("domain:", line);
		String lon = getFieldValue("lon:", line);
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our non-enumerated session field values
		sessionBuilder.setApiKey(getValidatedFieldValue(apikey));
		sessionBuilder.setUserId(getValidatedFieldValue(uid));
		sessionBuilder.setResolution(getValidatedFieldValue(resolution));
		sessionBuilder.setUserAgent(getValidatedFieldValue(uagent));
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our event field values
		eventBuilder.setAb(getValidatedFieldValue(ab));
		eventBuilder.setLon(parseValidDouble(lon));
		eventBuilder.setLat(parseValidDouble(lat));
		eventBuilder.setAddress(getValidatedFieldValue(address));
		eventBuilder.setCity(getValidatedFieldValue(city));
		eventBuilder.setZip(getValidatedFieldValue(zip));
		eventBuilder.setState(getValidatedFieldValue(state));
		eventBuilder.setId(getIdList(idnumbers));
		eventBuilder.setStartIndex(parseValidInt(startIndex));
		eventBuilder.setTotal(parseValidInt(total));
		eventBuilder.setDomain(getValidatedFieldValue(domain));
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our session field values that are enumerated
		//Simply check to see if string exists in the input text
		//And enumeration based on string
		if (line.contains("activex:enabled"))
			sessionBuilder.setActivex(ActiveX.ENABLED);
		else
			sessionBuilder.setActivex(ActiveX.NOT_SUPPORTED);
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our event field values that are enumerated
		//Simply check to see if string exists in the input text
		//And set enumerations based on string
		if (line.contains("action:"))
			eventBuilder.setAction(Action.CLICK);
		else
			eventBuilder.setAction(Action.PAGE_VIEW);
		if (actionName.contains("PAGE"))
			eventBuilder.setActionName(ActionName.DEALER_PAGE_VIEWED);
		else if (actionName.contains("WEBSITE"))
			eventBuilder.setActionName(ActionName.DEALER_WEBSITE_VIEWED);
		else if (actionName.contains("UNHOSTED"))
			eventBuilder.setActionName(ActionName.VIEWED_CARFAX_REPORT_UNHOSTED);
		else if (actionName.contains("CARFAX"))
			eventBuilder.setActionName(ActionName.VIEWED_CARFAX_REPORT);
		else if (actionName.contains("MORE"))
			eventBuilder.setActionName(ActionName.MORE_PHOTOS_VIEWED);
		else
			eventBuilder.setActionName(ActionName.NONE);
		if (getFieldValue("vertical:", line).toUpperCase().equals("CARS"))
			eventBuilder.setVertical(Vertical.CARS);	
		if (eventType.length() == 0)
			eventBuilder.setEventType(EventType.SRP);
		else if (eventType.contains("ACTION"))
			eventBuilder.setEventType(EventType.ACTION);
		else
			eventBuilder.setEventType(EventType.VDP);

		if (line.contains("phone_type:tracked"))
			eventBuilder.setPhoneType(PhoneType.TRACKED);
		//////////////////////////////////////////////////////////


		//Set our output key using concatenated uid and apikey
		word.set(uid+":"+apikey);


		//Create eventList out of our event and store in the session
		List<Event> eventList = new ArrayList<Event>();
		eventList.add(eventBuilder.build());
		sessionBuilder.setEvents(eventList);

		//Write key and session (wrapped in avro value) to context
		context.write( word, new AvroValue<Session>(sessionBuilder.build()));

	}

	//Parse valid double and return 0.0 if invalid
	private Double parseValidDouble(String key){
		if (getValidatedFieldValue(key) != null)
			return Double.parseDouble(key);
		else
			return 0.0;
	}

	//Parse valid long and return 0 if invalid
	private Long parseValidLong(String key){
		if (getValidatedFieldValue(key) != null)
			return Long.parseLong(key);
		else
			return (long)0;
	}

	//Parse valid integer and return 0 if invalid
	private Integer parseValidInt(String key) {
		if (getValidatedFieldValue(key) != null)
			return Integer.parseInt(key);
		else
			return 0;
	}

	//Takes a string of idnumbers and parses into a Long type ArrayList of Id's
	private List<Long> getIdList(String idnumbers) {
		//Tokenizer for the idnumbers
		StringTokenizer tokenizer = new StringTokenizer(idnumbers);
		List<Long> idList = new ArrayList<Long>(); //Stores our list of Id's
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			long temp = Long.parseLong(token); //Parse the current token
			idList.add(temp); //Store to list
		}
		return idList;
	}

	//Validates a field value and determines if
	private String getValidatedFieldValue(String fieldValue){
		//If the length of the field value is 0, it must be invalid
		//If the field value contains the object object value, it should be null
		if (fieldValue.length() > 0 && !(fieldValue.toUpperCase().contains("[OBJECT OBJECT]")))
			return fieldValue;
		else
			return null;
	}

	//Get the value of the field passed in as the fieldKey
	//Uses the line (input) to find the value
	private String getFieldValue(String fieldKey, String line){
		String result = "";

		//Find position occurrence of the fieldKey
		int index = line.indexOf(fieldKey);

		//Grab the initial offset 
		int offset  = fieldKey.length();

		//Store size of line
		int sizeOfLine = line.length();

		//Iterate through the line until we break
		System.out.println("entering while loop");
		while(true){

			//If the index is -1, the fieldKey did not appear
			//If the index+offset > size of line, we have reached end of file
			if (index == -1  || (index+offset > sizeOfLine-1))  break;

			//If the current character is '|', we have reached the 
			//end of that field value
			if (line.charAt(index+offset) == '|') break;

			//Once we get here, we can safely add our current char to result
			result += line.charAt(index + offset);

			//Increment our offset
			offset++;
		}
		System.out.println("broke while loop");
		//Return final field value
		return result;
	}
}
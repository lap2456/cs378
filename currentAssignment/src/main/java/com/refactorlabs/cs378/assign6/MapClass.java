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
		String userId= getFieldValue(0, line);
		String fullEventType = getFieldValue(1, line).toUpperCase();
		String page = getFieldValue(2, line);
		String referrer = getFieldValue(3, line);
		String referringDomain = getFieldValue(4, line);
		String eventTimeStamp = getFieldValue(5, line);
		String city = getFieldValue(6, line);
		String region = getFieldValue(7, line);
		String vin = getFieldValue(8, line);
		String condition = getFieldValue(9, line);
		String year = getFieldValue(10, line);
		String make = getFieldValue(11, line);
		String model = getFieldValue(12, line);
		String trim = getFieldValue(13, line);
		String bodyStyle = getFieldValue(14, line);
		String subtrim = getFieldValue(15, line);
		String cabStyle = getFieldValue(16, line);
		String initialPrice = getFieldValue(17, line);
		String mileage = getFieldValue(18, line);
		String mpg = getFieldValue(19, line);
		String exteriorColor = getFieldValue(20, line);
		String interiorColor = getFieldValue(21, line);
		String engineDisplacement = getFieldValue(22, line);
		String engine = getFieldValue(23, line);
		String transmission = getFieldValue(24, line);
		String driveType = getFieldValue(25, line);
		String fuel = getFieldValue(26, line);
		String imageCount = getFieldValue(27, line);
		String initialCarfaxFreeReport = getFieldValue(28, line);
		String carfaxOneOwner = getFieldValue(29, line);
		String initialCPO = getFieldValue(30, line);
		String features = getFieldValue(31, line);

		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our non-enumerated session field values
		sessionBuilder.setUserId(userId);
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our event field values
		eventBuilder.setEventTime(eventTimeStamp);
		eventBuilder.setPage(page);
		eventBuilder.setReferrer(referrer == "null" ? null : referrer);
		eventBuilder.setReferringDomain(referringDomain == "null" ? null : referringDomain);
		eventBuilder.setCity(city == "null" ? null : city);
		eventBuilder.setRegion(region == "null" ? null : region);
		eventBuilder.setVin(vin == "null" ? null : vin);
		eventBuilder.setYear(Integer.parseInt(year));
		eventBuilder.setMake(make);
		eventBuilder.setModel(model);
		eventBuilder.setTrim(trim == "" ? null : trim);
		eventBuilder.setSubtrim(subtrim);
		eventBuilder.setPrice(initialPrice == "0.0000" ? null : Float.parseFloat(initialPrice));
		eventBuilder.setMileage(Integer.parseInt(mileage));
		eventBuilder.setMpg(Integer.parseInt(mpg));
		eventBuilder.setExteriorColor(exteriorColor == "null" ? null : exteriorColor);
		eventBuilder.setInteriorColor(interiorColor == "null" ? null : interiorColor);
		eventBuilder.setEngineDisplacement(engineDisplacement == "null" ? null : engineDisplacement);
		eventBuilder.setEngine(engine == "null" ? null : engine);
		eventBuilder.setTransmission(transmission == "null" ? null : transmission);
		eventBuilder.setDriveType(driveType == "null" ? null : driveType);
		eventBuilder.setFuel(fuel == "null" ? null : fuel);
		eventBuilder.setImageCount(imageCount == "0" ? null : Integer.parseInt(imageCount));
		eventBuilder.setFreeCarfaxReport(initialCarfaxFreeReport == "t" ? true : false);
		eventBuilder.setCarfaxOneOwner(carfaxOneOwner == "t" ? true : false);
		eventBuilder.setCpo(initialCPO == "t" ? true : false);

		List<String> featuresList = new ArrayList<String>();
		String[] featuresSet = features.toString().split(":");
		for(int i=0; i<featuresSet.length; ++i){
			featuresList.add(featuresSet[i]);
		}
		eventBuilder.setFeatures(featuresList);
		//////////////////////////////////////////////////////////
		
		//////////////////////////////////////////////////////////
		//Set all of our event field values that are enumerated
		//Simply check to see if string exists in the input text
		//And set enumerations based on string
		//EventType
		if (fullEventType.startsWith("CHANGE"))
			eventBuilder.setEventType(EventType.CHANGE);
		else if (fullEventType.startsWith("CLICK"))
			eventBuilder.setEventType(EventType.CLICK);
		else if (fullEventType.startsWith("CONTACT FORM"))
			eventBuilder.setEventType(EventType.CONTACT_FORM_STATUS);
		else if (fullEventType.startsWith("EDIT"))
			eventBuilder.setEventType(EventType.EDIT);
		else if (fullEventType.startsWith("SHARE"))
			eventBuilder.setEventType(EventType.SHARE);
		else if (fullEventType.startsWith("SHOW"))
			eventBuilder.setEventType(EventType.SHOW);
		else if (fullEventType.startsWith("SUBMIT"))
			eventBuilder.setEventType(EventType.SUBMIT);
		else
			eventBuilder.setEventType(EventType.VISIT);
		//EventSubtype
		if (fullEventType.endsWith("CONTACT FORM TYPE"))
			eventBuilder.setEventSubtype(EventSubtype.CONTACT_FORM_TYPE);
		else if(fullEventType.endsWith("ALTERNATIVES"))
			eventBuilder.setEventSubtype(EventSubtype.ALTERNATIVE);
		else if(fullEventType.endsWith("CONTACT BANNER"))
			eventBuilder.setEventSubtype(EventSubtype.CONTACT_BANNER);
		else if(fullEventType.endsWith("CONTACT BUTTON"))
			eventBuilder.setEventSubtype(EventSubtype.CONTACT_BUTTON);
		else if(fullEventType.endsWith("DEALER PHONE"))
			eventBuilder.setEventSubtype(EventSubtype.DEALER_PHONE);
		else if(fullEventType.endsWith("FEATURES"))
			eventBuilder.setEventSubtype(EventSubtype.FEATURES);
		else if(fullEventType.endsWith("GET DIRECTIONS"))
			eventBuilder.setEventSubtype(EventSubtype.GET_DIRECTIONS);
		else if(fullEventType.endsWith("SHOW MORE BADGES"))
			eventBuilder.setEventSubtype(EventSubtype.SHOW_MORE_BADGES);
		else if(fullEventType.endsWith("TEST DRIVE LINK"))
			eventBuilder.setEventSubtype(EventSubtype.TEST_DRIVE_LINK);
		else if(fullEventType.endsWith("VEHICLE HISTORY REPORT LINK"))
			eventBuilder.setEventSubtype(EventSubtype.VEHICLE_HISTORY);
		else if(fullEventType.endsWith("FORM ERROR"))
			eventBuilder.setEventSubtype(EventSubtype.FORM_ERROR);
		else if(fullEventType.endsWith("FORM SUCCESS"))
			eventBuilder.setEventSubtype(EventSubtype.FORM_SUCCESS);
		else if(fullEventType.endsWith("CONTACT FORM"))
			eventBuilder.setEventSubtype(EventSubtype.CONTACT_FORM);
		else if(fullEventType.endsWith("BADGE DETAIL"))
			eventBuilder.setEventSubtype(EventSubtype.BADGE_DETAIL);
		else if(fullEventType.endsWith("PHOTO MODAL"))
			eventBuilder.setEventSubtype(EventSubtype.PHOTO_MODAL);
		else if(fullEventType.endsWith("BADGES"))
			eventBuilder.setEventSubtype(EventSubtype.BADGES);
		else
			eventBuilder.setEventSubtype(EventSubtype.MARKET_REPORT);
		//Condition
		if(condition.equals("CPO"))
			eventBuilder.setCondition(Condition.CPO);
		else if(condition.equals("New"))
			eventBuilder.setCondition(Condition.New);
		else
			eventBuilder.setCondition(Condition.Used);
		//BodyStyle
		if(bodyStyle.equals("Chassis"))
			eventBuilder.setBodyStyle(BodyStyle.Chassis);
		else if(bodyStyle.equals("Convertible"))
			eventBuilder.setBodyStyle(BodyStyle.Convertible);
		else if(bodyStyle.equals("Coupe"))
			eventBuilder.setBodyStyle(BodyStyle.Coupe);
		else if(bodyStyle.equals("Hatchback"))
			eventBuilder.setBodyStyle(BodyStyle.Hatchback);
		else if(bodyStyle.equals("Minivan"))
			eventBuilder.setBodyStyle(BodyStyle.Minivan);
		else if(bodyStyle.equals("Pickup"))
			eventBuilder.setBodyStyle(BodyStyle.Pickup);
		else if(bodyStyle.equals("SUV"))
			eventBuilder.setBodyStyle(BodyStyle.SUV);
		else if(bodyStyle.equals("Sedan"))
			eventBuilder.setBodyStyle(BodyStyle.Sedan);
		else if(bodyStyle.equals("Van"))
			eventBuilder.setBodyStyle(BodyStyle.Van);
		else if(bodyStyle.equals("Wagon"))
			eventBuilder.setBodyStyle(BodyStyle.Wagon);
		else
			eventBuilder.setBodyStyle(null);
		//CabStyle
		if(cabStyle.equals("Regular Cab"))
			eventBuilder.setCabStyle(CabStyle.Regular);
		else if(cabStyle.equals("Extended Cab"))
			eventBuilder.setCabStyle(CabStyle.Extended);
		else if(cabStyle.equals("Crew Cab"))
			eventBuilder.setCabStyle(CabStyle.Crew);
		else
			eventBuilder.setCabStyle(null);
		//////////////////////////////////////////////////////////


		//Set our output key using userId
		word.set(userId);

		//Create eventList out of our event and store in the session
		List<Event> eventList = new ArrayList<Event>();
		eventList.add(eventBuilder.build());
		sessionBuilder.setEvents(eventList);

		//Write key and session (wrapped in avro value) to context
		context.write( word, new AvroValue<Session>(sessionBuilder.build()));
	}

	//Find a field based on how many tabs are before it
	private String getFieldValue(int index, String line){
		return line.split("\t")[index];
	}
}
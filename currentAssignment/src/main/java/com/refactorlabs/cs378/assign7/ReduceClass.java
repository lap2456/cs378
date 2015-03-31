package com.refactorlabs.cs378.assign7;

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
extends Reducer<Text, AvroValue<VinImpressionCounts>,
Text, AvroValue<VinImpressionCounts>> {

	@Override
	public void reduce(Text key, AvroValue<VinImpressionCounts> values, Context context)
			throws IOException, InterruptedException {
		
		VinImpressionCounts.Builder finalVinImpression = VinImpressionCounts.newBuilder();

		List<VinImpressionCounts> impList = new ArrayList<VinImpressionCounts>();

		for (AvroValue<VinImpressionCounts> value : values){
			if(value.hasUniqueUser())
				finalVinImpression.setUniqueUser(value.getUniqueUser());
			if(value.hasClicks())
				finalVinImpression.setClicks(value.getClicks());
			if(value.hasShareMarketReport())
				finalVinImpression.setShareMarketReport(value.getShareMarketReport());
			if(value.hasSubmitContactForm())
				finalVinImpression.setSubmitContactForm(value.getSubmitContactForm());
			if(value.hasUniqueUserVdpView())
				finalVinImpression.setUniqueUserVdpView(value.getUniqueUserVdpView());
		}
		
		if(finalVinImpression.hasUniqueUser())
			context.write(key, new AvroValue<VinImpressionCounts>(finalVinImpression.build()));

	}
}
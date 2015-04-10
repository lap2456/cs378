package com.refactorlabs.cs378.assign9;

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
public class SharerReduceClass
extends Reducer<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>,
AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {

	@Override
	public void reduce(AvroKey<ClickSubtypeStatisticsKey> key, Iterable<AvroValue<ClickSubtypeStatisticsData>> values, Context context)
			throws IOException, InterruptedException {
		
		ClickSubtypeStatisticsData.Builder finalSubtypeData = ClickSubtypeStatisticsData.newBuilder();

		for (AvroValue<ClickSubtypeStatisticsData> value : values){
			ClickSubtypeStatisticsData valueDatum = value.datum();
			if(!finalSubtypeData.hasSessionCount())
				finalSubtypeData.setSessionCount(0L);
			finalSubtypeData.setSessionCount(finalSubtypeData.getSessionCount() + valueDatum.getSessionCount());

			if(!finalSubtypeData.hasTotalCount())
				finalSubtypeData.setTotalCount(0L);
			finalSubtypeData.setTotalCount(finalSubtypeData.getTotalCount() + valueDatum.getTotalCount());
			
			if(!finalSubtypeData.hasSumOfSquares())
				finalSubtypeData.setSumOfSquares(0L);
			finalSubtypeData.setSumOfSquares(finalSubtypeData.getSumOfSquares() + valueDatum.getSumOfSquares());
		}

		finalSubtypeData.setMean((double)(finalSubtypeData.getTotalCount()/finalSubtypeData.getSessionCount()));
		finalSubtypeData.setVariance((((double)finalSubtypeData.getSumOfSquares())/((double)finalSubtypeData.getSessionCount()) - (finalSubtypeData.getMean() * finalSubtypeData.getMean())));
		
		context.write(key, new AvroValue<ClickSubtypeStatisticsData>(finalSubtypeData.build()));
	}
}
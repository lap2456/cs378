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
public class ClickReduceClass
extends Reducer<AvroKey<clickSubtypeStatsKey>, AvroValue<ClickSubtypeStatsData>,
AvroKey<clickSubtypeStatsKey>, AvroValue<ClickSubtypeStatsData>> {

	@Override
	public void reduce(AvroKey<clickSubtypeStatsKey> key, Iterable<AvroValue<ClickSubtypeStatsData>> values, Context context)
			throws IOException, InterruptedException {
		
		ClickSubtypeStatsData.Builder finalSubtypeData = ClickSubtypeStatsData.newBuilder();

		for (AvroValue<ClickSubtypeStatsData> value : values){
			ClickSubtypeStatsData valueDatum = value.datum();
			if(!finalSubtypeData.hasSessionCount())
				finalSubtypeData.setSessionCount(0L);
			finalSubtypeData.setSessionCount(finalSubtypeData.getSessionCount() + valueDatum.getSessionCount());
			if(!finalSubtypeData.hasTotalCount())
				finalSubtypeData.setTotalCount(0L);
			finalSubtypeData.setTotalCount(finalSubtypeData.getTotalCount() + valueDatum.getTotalCount());
		}

		finalSubtypeData.setSumOfSquares(finalSubtypeData.getSessionCount() * finalSubtypeData.getSessionCount());
		finalSubtypeData.setVariance((double)(finalSubtypeData.getSumOfSquares()/(finalSubtypeData.getTotalCount()-1L)));
		finalSubtypeData.setMean((double)(finalSubtypeData.getSessionCount()/finalSubtypeData.getTotalCount()));
		
		context.write(key, new AvroValue<ClickSubtypeStatsData>(finalSubtypeData.build()));
	}
}
package com.refactorlabs.cs378.assign7;

import org.apache.avro.Schema;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * UserSessions main driver program for creating user session objects
 * using Avro
 * 
 * @author Louis Pujol (louispujol@yahoo.com)
 * @author David Franke (dfranke@cs.utexas.edu)
 *
 *
 */
public class UserSessions extends Configured implements Tool {

	
	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: UserSessions <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "UserSessions");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(UserSessions.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");

		// Specify the Map
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		job.setMapperClass(SessionMapClass.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());
		AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());
		
		// Specify the Reduce
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());

		//Process command line input and send to appropriate mapper 
		//First input line is impression mapper, second is lead mapper
		String[] inputPaths = appArgs[0].split(",");
		MultipleInputs.addInputPath(job, new Path(inputPaths[0]), AvroKeyValueInputFormat.class, SessionMapClass.class);
		MultipleInputs.addInputPath(job, new Path(inputPaths[1]), TextInputFormat.class, VinMapClass.class);

		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new UserSessions(), args);
		System.exit(res);
	}

}
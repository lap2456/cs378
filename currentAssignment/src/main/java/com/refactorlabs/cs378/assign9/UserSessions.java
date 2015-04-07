package com.refactorlabs.cs378.assign9;

import org.apache.avro.Schema;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "UserSessions");
		Job submitterJob = new Job(conf, "UserSessionsSubmitter");
		Job clickerJob = new Job(conf, "UserSessionsClicker");
		Job sharerJob = new Job(conf, "UserSessionsSharer");

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(UserSessions.class);
		submitterJob.setJarByClass(UserSessions.class);
		clickerJob.setJarByClass(UserSessions.class);
		sharerJob.setJarByClass(UserSessions.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");

		// Specify the Map
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		job.setMapperClass(SessionMapClass.class);
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());
		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

		submitterJob.setInputFormatClass(AvroKeyValueInputFormat.class);
		submitterJob.setMapperClass(SubmitterMapClass.class);
		AvroJob.setInputKeySchema(submitterJob, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(submitterJob, Session.getClassSchema());
		AvroJob.setMapOutputKeySchema(submitterJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(submitterJob, ClickSubtypeStatisticsData.getClassSchema());
		submitterJob.setReducerClass(ClickReduceClass.class);
		submitterJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		AvroJob.setOutputKeySchema(submitterJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setOutputValueSchema(submitterJob, ClickSubtypeStatisticsData.getClassSchema());
		FileInputFormat.addInputPath(submitterJob, new Path(appArgs[1] + "/Submitter-m-00000.avro"));
		FileInputFormat.addInputPath(submitterJob, new Path(appArgs[1] + "/Submitter-m-00001.avro"));
		FileInputFormat.addInputPath(submitterJob, new Path(appArgs[1] + "/Submitter-m-00002.avro"));
		
		clickerJob.setInputFormatClass(AvroKeyValueInputFormat.class);
		clickerJob.setMapperClass(ClickerMapClass.class);
		AvroJob.setInputKeySchema(clickerJob, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(clickerJob, Session.getClassSchema());
		AvroJob.setMapOutputKeySchema(clickerJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(clickerJob, ClickSubtypeStatisticsData.getClassSchema());
		clickerJob.setReducerClass(ClickReduceClass.class);
		clickerJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		AvroJob.setOutputKeySchema(clickerJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setOutputValueSchema(clickerJob, ClickSubtypeStatisticsData.getClassSchema());
		FileInputFormat.addInputPath(clickerJob, new Path(appArgs[1] + "/Clicker-m-00000.avro"));
		FileInputFormat.addInputPath(clickerJob, new Path(appArgs[1] + "/Clicker-m-00001.avro"));
		FileInputFormat.addInputPath(clickerJob, new Path(appArgs[1] + "/Clicker-m-00002.avro"));

		sharerJob.setInputFormatClass(AvroKeyValueInputFormat.class);
		sharerJob.setMapperClass(SharerMapClass.class);
		AvroJob.setInputKeySchema(sharerJob, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(sharerJob, Session.getClassSchema());
		AvroJob.setMapOutputKeySchema(sharerJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(sharerJob, ClickSubtypeStatisticsData.getClassSchema());
		sharerJob.setReducerClass(ClickReduceClass.class);
		sharerJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		AvroJob.setOutputKeySchema(sharerJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setOutputValueSchema(sharerJob, ClickSubtypeStatisticsData.getClassSchema());
		FileInputFormat.addInputPath(sharerJob, new Path(appArgs[1] + "/Sharer-m-00000.avro"));
		FileInputFormat.addInputPath(sharerJob, new Path(appArgs[1] + "/Sharer-m-00001.avro"));
		FileInputFormat.addInputPath(sharerJob, new Path(appArgs[1] + "/Sharer-m-00002.avro"));

		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());

		AvroMultipleOutputs.addNamedOutput(job, "Submitter", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Sharer", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Clicker", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Shower", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Visitor", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Other", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());

		// Grab the input file and output directory from the command line.
		String[] inputPaths = appArgs[0].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
		FileOutputFormat.setOutputPath(submitterJob, new Path(appArgs[1] + "SubmitterData"));
		FileOutputFormat.setOutputPath(clickerJob, new Path(appArgs[1] + "ClickerData"));
		FileOutputFormat.setOutputPath(sharerJob, new Path(appArgs[1] + "SharerData"));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		//create the 3 jobs

		submitterJob.submit();
		clickerJob.submit();
		sharerJob.submit();
		
		//create the final job
		/*
		Job aggregatorJob = new Job(conf, "UserSessionsAggregator");
		aggregatorJob.setJarByClass(UserSessions.class);
		aggregatorJob.setInputFormatClass(AvroKeyValueInputFormat.class);
		aggregatorJob.setMapperClass(AggregatorMapClass.class);
		AvroJob.setInputKeySchema(aggregatorJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setInputValueSchema(aggregatorJob, ClickSubtypeStatisticsData.getClassSchema());
		aggregatorJob.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(aggregatorJob, ClickSubtypeStatisticsData.getClassSchema());
		aggregatorJob.setReducerClass(AggregatorReduceClass.class);
		aggregatorJob.setOutputFormatClass(TextOutputFormat.class);
		aggregatorJob.setOutputKeyClass(Text.class);
		AvroJob.setOutputValueSchema(aggregatorJob, ClickSubtypeStatisticsData.getClassSchema());
		FileInputFormat.addInputPath(aggregatorJob, new Path(appArgs[1] + "/SubmitterData/" + "part-r-00000.avro"))
		*/
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
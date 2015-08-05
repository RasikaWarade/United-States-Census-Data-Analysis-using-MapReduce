package cs455.hadoop.census;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs455.hadoop.gigasort.GSMap;
import cs455.hadoop.gigasort.GSPartitioner;
import cs455.hadoop.gigasort.GSReducer;
import cs455.hadoop.gigasort.Gigasort;

public class CensusJob {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			Configuration conf = new Configuration();
			// Give the MapRed job a name. You'll see this name in the Yarn
			Job job = Job.getInstance(conf, "CensusData");
			// Current class.
			job.setJarByClass(CensusJob.class);
			// Mapper
			job.setMapperClass(CensusMapper.class);

			// Reducer
			job.setReducerClass(CensusReducer.class);
			job.setNumReduceTasks(52);// For number of reduce tasks

			// Outputs from the Mapper.
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(CensusDataExtract.class);

			//Partitioner Class
			job.setPartitionerClass(CensusPartitioner.class); // Partition class

			// Outputs from Reducer. It is sufficient to set only the following
			// two properties		

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// path to input in HDFS			
			FileInputFormat.addInputPath(job, new Path(args[0]));

			// path to output in HDFS
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			//Check if first job is completed
			Boolean success=job.waitForCompletion(true);
			if(success){


				Job newjob = Job.getInstance(conf, "CensusDataAdd");

				// Current class.
				newjob.setJarByClass(CensusJob.class);
				// Mapper
				newjob.setMapperClass(SecondMapper.class);


				// Reducer
				newjob.setReducerClass(SecondReducer.class);


				// Outputs from the Mapper.
				newjob.setMapOutputKeyClass(Text.class);
				newjob.setMapOutputValueClass(Text.class);


				// Outputs from Reducer. It is sufficient to set only the following
				// two properties

				newjob.setOutputKeyClass(Text.class);
				newjob.setOutputValueClass(Text.class);

				// path to input in HDFS
				FileInputFormat.addInputPath(newjob, new Path(args[1]));
				// path to output in HDFS
				FileOutputFormat.setOutputPath(newjob, new Path(args[1]+"_tmp")); //Tmp folder to store the output created by first reducer

				// Block until the job is completed.
				System.exit(newjob.waitForCompletion(true) ? 0 : 1);
			}	

		} catch (IOException e) {
			System.err.println(e.getMessage());
		} catch (InterruptedException e) {
			System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			System.err.println(e.getMessage());
		}

	}
}

package cs455.hadoop.census;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SecondMapper extends
Mapper<LongWritable, Text, Text, Text> {



	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {


		String line = value.toString(); //theRecord
		String tokens[]=line.split("\t");
		Text word=new Text("one");

		context.write(word,new Text(tokens[0]+"#"+tokens[1])); // US STATE # PER-STATE VALUES



	}
}
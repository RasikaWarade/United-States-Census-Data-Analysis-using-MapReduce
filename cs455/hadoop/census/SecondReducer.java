
package cs455.hadoop.census;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SecondReducer extends
Reducer<Text, Text, Text, Text> {

	private Text word = new Text();

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		//Output the values
				
		String output="";
		output=String.format("%-12s%-9s%-9s%-9s%-12s%-12s%-12s%-12s%-12s%-12s%-12s%-12s%-9s%-9s%-9s", "US State","OWNER%","RENTER%","MALES NEVER MARRIED%","FEMALES NEVER MARRIED%","MALES BELOW 18%",
				"FEMALES BELOW 18%","MALES(19-29)%","FEMALES(19-29)%","MALES(30-39)%","FEMALES(30-39)%","URBAN%","RURAL%","VALUE RANGE","RENT RANGE");//"MALES NEVER MARRIED","FEMALES NEVER MARRIED","BELOW 18");
		context.write(word, new Text(output));
		ArrayList<Integer> population85=new ArrayList<Integer>();
		ArrayList<String> USstates=new ArrayList<String>();
		ArrayList<Long> rooms95=new ArrayList<Long>();
		
		
		for(Text val: values){
			
			String tokens1[]=val.toString().split("#");
			USstates.add(tokens1[0].trim());
			String Record="";
			String HRange="("+tokens1[13]+"-"+tokens1[14]+")";
			String RRange="("+tokens1[15]+"-"+tokens1[16]+")";
			
			Record=String.format("%-12s%-12s%-9s%-9s%-12s%-12s%-15s%-15s%-15s%-15s%-12s%-12s%-20s%-20s%-10s", tokens1[0],tokens1[1],tokens1[2],tokens1[3],tokens1[4],tokens1[5],tokens1[6],tokens1[7],tokens1[8],
					tokens1[9],tokens1[10],tokens1[11],tokens1[12],HRange,RRange);
			context.write(word,new Text( Record));
			double valr = Double.parseDouble(tokens1[18].trim());
			population85.add((int) Math.floor(valr));//Percentage for >85
			rooms95.add(Long.parseLong(tokens1[17].trim()));//ROOMS
			

		}
		
		//QUestion 8
	//	System.out.println(USstates);
	//	System.out.println(population85);
		
		ArrayList<Integer> highestPopulation85=new ArrayList<Integer>(population85);
		Collections.sort(highestPopulation85);
		
	//	System.out.println(highestPopulation85);
		
		int max=highestPopulation85.get(highestPopulation85.size()-1);
		
	//	System.out.println(max);
		
		int H=population85.indexOf(max);
		
	//	System.out.println(H);
	//	System.out.println(USstates.get(H));
				
		context.write(word,new Text( USstates.get(H)+" "+max));
		
		//Question 7
	//	System.out.println(rooms95);
		ArrayList<Long> rooms95avg=new ArrayList<Long>(rooms95);
		Collections.sort(rooms95avg);
		
	//	System.out.println(rooms95avg);
		
		//49th index is the 95th percentile
		
		context.write(word, new Text(rooms95avg.get(49).toString()));
		
	}
}


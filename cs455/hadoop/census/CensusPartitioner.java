package cs455.hadoop.census;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CensusPartitioner extends Partitioner<Text, CensusDataExtract> {

	@Override
	public int getPartition(Text key, CensusDataExtract value,
			int numofReduceTasks) {

		//Creating dictionary of states
		States stt=new States();
		HashMap<String, Integer> states =stt.getList();

		// Partition based on the number of related to states
		Iterator<String> keySetIterator = states.keySet().iterator();

		while(keySetIterator.hasNext()){
			String K = keySetIterator.next();
			if(K.contentEquals(key.toString()))
				return (states.get(K)-1);
		}
		return 0;
	}

}


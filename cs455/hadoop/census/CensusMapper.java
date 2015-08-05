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

public class CensusMapper extends
Mapper<LongWritable, Text, Text, CensusDataExtract> {



	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {


		String line = value.toString(); //theRecord


		//Unique fields for both the segments		
		String state=line.substring(8, 10);	//State
		int level=Integer.parseInt(line.substring(10, 13));	//SummaryLevel

		Long logNum=Long.parseLong(line.substring(18,24));	//LogicalRecordNumber
		int part=Integer.parseInt(line.substring(24,28));	//PartNumber

		//Access only Summary Level 100		
		if(level==100){
			CensusDataExtract Data=new CensusDataExtract();

			//Extract unique fields for both the segments
			Data.setSummaryLevel(new IntWritable(level));
			Data.setState(new Text(state));
			Data.setLogicalRecord(new LongWritable(logNum));
			Data.setPart( new IntWritable(part));

			//Data Segment 1
			if(Data.getPart().get()==1){

				//Marital status
				int indexM=4422;
				int indexF=4467;
				int incr=9;
				Long malesMaritalStatus=new Long("0");
				Long malesNever=Long.parseLong(line.substring(indexM, indexM+incr));	//MalesNeverMarried
				for(int i=0;i<4;i++){
					malesMaritalStatus+=Long.parseLong(line.substring(indexM, indexM+incr));	//AddingMaleCategories
					indexM+=incr;
				}

				Long femalesMaritalStatus=new Long("0");
				Long femalesNever=Long.parseLong(line.substring(indexF, indexF+incr));	//MalesNeverMarried

				for(int k=0;k<4;k++){
					femalesMaritalStatus+=Long.parseLong(line.substring(indexF, indexF+incr));	//AddingMaleCategories
					indexF+=incr;
				}

				Data.setMaleNeverMarried(new LongWritable(malesNever));
				Data.setTotalMales(new LongWritable(malesMaritalStatus));

				Data.setFemaleNeverMarried(new LongWritable(femalesNever));
				Data.setTotalFemales(new LongWritable(femalesMaritalStatus));
				
				//Ages
				
				int indexM1=3864;				
				int indexF1=4143;
				
				Long malesAge=new Long("0");
				Long femalesAge=new Long("0");
				
				Long malesAge18=new Long("0");
				Long malesAge29=new Long("0");
				Long malesAge39=new Long("0");
				Long malesAge85=new Long("0");
				
				Long femalesAge18=new Long("0");
				Long femalesAge29=new Long("0");
				Long femalesAge39=new Long("0");
				Long femalesAge85=new Long("0");
				
				for(int j=0;j<31;j++){
					//index 0 to 12 is below 18
					if(j<13){
						malesAge18+=Long.parseLong(line.substring(indexM1, indexM1+incr));
						femalesAge18+=Long.parseLong(line.substring(indexF1, indexF1+incr));
						
					}
					
					//index 13 to 17 is between 19 and 29
					else if(j>12 && j<18){
						malesAge29+=Long.parseLong(line.substring(indexM1, indexM1+incr));
						femalesAge29+=Long.parseLong(line.substring(indexF1, indexF1+incr));
						
					}
					//index 18 to 19 is between 30 and 39
					else if(j==18 || j==19){
						malesAge39+=Long.parseLong(line.substring(indexM1, indexM1+incr));
						femalesAge39+=Long.parseLong(line.substring(indexF1, indexF1+incr));
						
					}
					else{
						malesAge85+=Long.parseLong(line.substring(indexM1, indexM1+incr));
						femalesAge85+=Long.parseLong(line.substring(indexF1, indexF1+incr));
						
					}
					
					
					indexM1+=incr;
					indexF1+=incr;
					
					
				}
				
				malesAge=malesAge18+malesAge29+malesAge39+malesAge85;
				femalesAge=femalesAge18+femalesAge29+femalesAge39+femalesAge85;
				
				Data.setMalesAge(new LongWritable(malesAge));
				Data.setFemalesAge(new LongWritable(femalesAge));
				Data.setMalesAge18(new LongWritable(malesAge18));
				Data.setMalesAge29(new LongWritable(malesAge29));
				Data.setMalesAge39(new LongWritable(malesAge39));
				Data.setMalesAge85(new LongWritable(malesAge85));
				Data.setFemalesAge18(new LongWritable(femalesAge18));
				Data.setFemalesAge29(new LongWritable(femalesAge29));
				Data.setFemalesAge39(new LongWritable(femalesAge39));
				Data.setFemalesAge85(new LongWritable(femalesAge85));
				//System.out.println(Data.getFemalesAge85());

			}

			//Data Segment 2
			if(Data.getPart().get()==2){
				Long owner=Long.parseLong(line.substring(1803, 1812));	//Owner
				Long renter=Long.parseLong(line.substring(1812, 1821));	//Renter

				Data.setOwner(new LongWritable(owner));
				Data.setRenter(new LongWritable(renter));

				//Urban vs Rural
				int index=1857;
				int incr=9;
				Long totalHouses=new Long("0");
				Long urban=new Long("0");
				Long rural=new Long("0");
				for(int i=0;i<4;i++){
					if(i==0 ||i==1){

						urban+=Long.parseLong(line.substring(index, index+incr));	//Urban
					}
					if(i==2){
						rural+=Long.parseLong(line.substring(index, index+incr));	//Rural
					}
					totalHouses+=Long.parseLong(line.substring(index, index+incr));	//AddHouses
					index+=incr;
				}
				Data.setUrban(new LongWritable(urban));
				Data.setRural(new LongWritable(rural));
				Data.setHouses(new LongWritable(totalHouses));
				
				//House Value
				int indexH=2928;
				ArrayList<LongWritable> valueList = new ArrayList<LongWritable>();
				
				for(int k=0;k<20;k++){
					Long values=Long.parseLong(line.substring(indexH, indexH+incr));
					valueList.add(new LongWritable(values));
					indexH+=incr;
					
				}
				
				Data.setValueList(new Text(valueList.toString()));
				
				
				//Contact Rent
				int indexR=3450;
				//int incr=9;
				ArrayList<LongWritable> rentList = new ArrayList<LongWritable>();
				
				for(int k=0;k<16;k++){
					Long values=Long.parseLong(line.substring(indexR, indexR+incr));
					rentList.add(new LongWritable(values));
					indexR+=incr;
					
				}
				Data.setRentList(new Text(rentList.toString()));
				
			
				//House ROOMS
				int indexRo=2388;
				ArrayList<LongWritable> roomsList = new ArrayList<LongWritable>();
				
				for(int k=0;k<9;k++){
					Long values=Long.parseLong(line.substring(indexRo, indexRo+incr));
					roomsList.add(new LongWritable(values));
					indexRo+=incr;
					
				}
				
				Data.setRoomsList(new Text(roomsList.toString()));
				
				
			}
			context.write(new Text(state), Data);
		}

	}
}
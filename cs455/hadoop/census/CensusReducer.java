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

public class CensusReducer extends
Reducer<Text, CensusDataExtract, Text, Text> {

	private Text word = new Text();

	protected void reduce(Text key, Iterable<CensusDataExtract> values, Context context)
			throws IOException, InterruptedException {
		DecimalFormat twoDec = new DecimalFormat("#.00");

		//Tenure
		double totalOwner=0;
		double totalRenter=0;

		//Maritalstatus
		double totalMalesNeverMarried=0;
		double totalMales=0;

		double totalFemalesNeverMarried=0;
		double totalFemales=0;


		//Urban vs Rural
		double urban=0;
		double rural=0;
		double totalHouses=0;

		//Ages
		double totalMaleAges18=0;
		double totalMaleAges29=0;
		double totalMaleAges39=0;
		double totalMaleAges85=0;
		double totalMalesAges=0;

		double totalFemaleAges18=0;
		double totalFemaleAges29=0;
		double totalFemaleAges39=0;
		double totalFemaleAges85=0;
		double totalFemalesAges=0;


		//House Values
		String valueList;

		ArrayList<Long> totalValuetList = new ArrayList<Long>();
		for(int i=0;i<20;i++){
			totalValuetList.add((long) 0);
		}
		Long HouseValues=(long) 0;
		Long totalValues=(long) 0;
		
		//Rent Values
		String rentList;

		ArrayList<Long> totalRenttList = new ArrayList<Long>();
		for(int i=0;i<16;i++){
			totalRenttList.add((long) 0);
		}
		int count=0;
		double RentValues=0;
		double totalRent=0;
		
		//HOUSE ROOMS
		String roomsList="";
		ArrayList<Long> totalRoomsList = new ArrayList<Long>();
		for(int i=0;i<9;i++){
			totalRoomsList.add((long) 0);
		}

		for(CensusDataExtract censusD: values){
			//context.write(key, new Text(censusD.toString()));

			//Add all the values for Owner and renter
			totalOwner+=censusD.getOwner().get();
			totalRenter+=censusD.getRenter().get();

			//Add all the values for Males Never Married and Females Never Married, Plus get the total Marital Status for both
			totalMalesNeverMarried+=censusD.getMaleNeverMarried().get();
			totalMales+=censusD.getTotalMales().get();
			totalFemalesNeverMarried+=censusD.getFemaleNeverMarried().get();
			totalFemales+=censusD.getTotalFemales().get();

			//Add all the rural and urban households
			urban+=censusD.getUrban().get();
			rural+=censusD.getRural().get();
			totalHouses+=censusD.getHouses().get();

			//Add all the values based on ages for Male
			totalMaleAges18+=censusD.getMalesAge18().get();
			totalMaleAges29+=censusD.getMalesAge29().get();
			totalMaleAges39+=censusD.getMalesAge39().get();
			totalMaleAges85+=censusD.getMalesAge85().get();
			totalMalesAges+=censusD.getMalesAge().get();

			//Add all the values based on ages for Female
			totalFemaleAges18+=censusD.getFemalesAge18().get();
			totalFemaleAges29+=censusD.getFemalesAge29().get();
			totalFemaleAges39+=censusD.getFemalesAge39().get();
			totalFemaleAges85+=censusD.getFemalesAge85().get();
			totalFemalesAges+=censusD.getFemalesAge().get();


			//Get the arraylist for the house value and rent ranges
			valueList=censusD.getValueList().toString();
			rentList=censusD.getRentList().toString();


			if(!valueList.contentEquals("")){

				valueList=valueList.toString().replace("[","");
				valueList=valueList.toString().replace("]","");
				valueList=valueList.toString().replace(" ","");

				rentList=rentList.toString().replace("[","");
				rentList=rentList.toString().replace("]","");
				rentList=rentList.toString().replace(" ","");

				String tokens1[]=valueList.trim().split(",");
				ArrayList<String> HOUSEVALUES = new ArrayList<String>(Arrays.asList(tokens1));

				String tokens2[]=rentList.trim().split(",");				
				ArrayList<String> RENTVALUES = new ArrayList<String>(Arrays.asList(tokens2));


				//Add all the ranges and store in the arraylist for the iterable valuelist
				ListIterator<Long> listIterator = totalValuetList.listIterator();
				for(int j=0;j<20;j++){
					if (listIterator.hasNext()) {
						listIterator.next();
						listIterator.set(totalValuetList.get(j)+(Long.parseLong(HOUSEVALUES.get(j).trim())));
						totalValues+=Long.parseLong(HOUSEVALUES.get(j).trim());
					}
				}

				//Add all the rent ranges and store in the arraylist for the iterable rentlist
				ListIterator<Long> listIterator2 = totalRenttList.listIterator();
				for(int j=0;j<16;j++){
					if (listIterator2.hasNext()) {
						listIterator2.next();
						listIterator2.set(totalRenttList.get(j)+(Long.parseLong(RENTVALUES.get(j).trim())));
						totalRent+=Long.parseLong(RENTVALUES.get(j).trim());
					}
				}

			}

			//Rooms
			roomsList=censusD.getRoomsList().toString();

			if(!roomsList.contentEquals("")){

				roomsList=roomsList.toString().replace("[","");
				roomsList=roomsList.toString().replace("]","");
				roomsList=roomsList.toString().replace(" ","");


				String tokens1[]=roomsList.trim().split(",");

				ArrayList<String> ROOMSVALUES = new ArrayList<String>(Arrays.asList(tokens1));

				//Add all the rooms and store in the arraylist for the iterable rentlist
				ListIterator<Long> listIterator = totalRoomsList.listIterator();
				for(int j=0;j<9;j++){
					if (listIterator.hasNext()) {
						listIterator.next();
						listIterator.set(totalRoomsList.get(j)+(Long.parseLong(ROOMSVALUES.get(j).trim())));

					}

				}

			}

		}

		//Calculate answer for Question 1
		double totalTenure=totalOwner+totalRenter;
		double ownPer=(totalOwner*100)/totalTenure;	
		double rentPer=(totalRenter*100)/totalTenure;
		Text out1=new Text(" Total Owner:"+totalOwner+ " Total Renter:"+totalRenter+ " Owner %:"+twoDec.format(ownPer)+" Renter %:"+twoDec.format(rentPer));	
		//context.write(key, out1);

		//Calculate answer for Question 2
		double malesPer=(totalMalesNeverMarried*100)/totalMales;
		double femalesPer=(totalFemalesNeverMarried*100)/totalFemales;
		Text out2=new Text("Males Never Married %:"+twoDec.format(malesPer)+" Females Never Married %:"+twoDec.format(femalesPer));	
		//context.write(key, out2);

		//Calculate answer for Question 4
		double urbanPer=(urban*100)/totalHouses;
		double ruralPer=(rural*100)/totalHouses;
		Text out3=new Text("Urban %:"+twoDec.format(urbanPer)+" Rural %:"+twoDec.format(ruralPer));	
		//context.write(key, out3);

		//Calculate answer for Question 3
		double male18Per=(totalMaleAges18*100)/totalMalesAges;
		double male29Per=(totalMaleAges29*100)/totalMalesAges;
		double male39Per=(totalMaleAges39*100)/totalMalesAges;

		double female18Per=(totalFemaleAges18*100)/totalFemalesAges;
		double female29Per=(totalFemaleAges29*100)/totalFemalesAges;
		double female39Per=(totalFemaleAges39*100)/totalFemalesAges;

		Text out4=new Text("A) Male18 %:"+twoDec.format(male18Per)+" Female18 %:"+twoDec.format(female18Per)+"B) Male29 %:"+twoDec.format(male29Per)+" Female29 %:"+twoDec.format(female29Per)+"C) Male39 %:"+twoDec.format(male39Per)+" Female39 %:"+twoDec.format(female39Per));	
		//context.write(key, out4);


		

		//Calculate answer for question 7
		//System.out.println(totalRoomsList);
		int totalrooms=0;
		int denrooms=0;
		for(int i=0;i<9;i++){
			denrooms+=totalRoomsList.get(i);
			totalrooms+=(i+1)*totalRoomsList.get(i);
			//System.out.println("total rooms:"+totalrooms+" i:"+(i+1)+" number:"+totalRoomsList.get(i));
		}
		//System.out.println("total rooms:"+totalrooms);
		if(denrooms!=0){
		totalrooms=totalrooms/denrooms;
		}
		
		//Calculate answer for question 8
		double people85Per=((totalMaleAges85+totalFemaleAges85)*100)/(totalMalesAges+totalFemalesAges);

		
		
		///Calculate answer for Question 5 and 6

		//System.out.println("TOTALHL"+totalValuetList);
		//System.out.println("TOTALRL"+totalRenttList);

		//Sort the list
		ArrayList<Long> totalValuetList2=new ArrayList<Long>(totalValuetList);

		ArrayList<Long> totalRenttList2=new ArrayList<Long>(totalRenttList);


		Collections.sort(totalValuetList2);
		Collections.sort(totalRenttList2);


		int MedianH=0;
		Long medianHValue=(long) 0;

		int MedianR=0;
		Long medianRValue=(long) 0;

		if(totalValuetList2.size()%2==0){
			MedianH=(totalValuetList2.size()/2 )+1;
		}
		else{
			MedianH=(totalValuetList2.size()/2 );
		}

		if(totalRenttList2.size()%2==0){
			MedianR=(totalRenttList2.size()/2 )+1;
		}
		else{
			MedianR=(totalRenttList2.size()/2 );
		}

		medianHValue=totalValuetList2.get(MedianH-1);
		medianRValue=totalRenttList2.get(MedianR-1);
		//System.out.println("MEDIAN HOUSE:"+medianHValue);
		//System.out.println("MEDIAN Rent:"+medianRValue);


		int H=totalValuetList.indexOf(medianHValue);

		int R=totalRenttList.indexOf(medianRValue);
		//System.out.println("H:"+H+" R:"+R);


		int HRANGE1=0;
		int HRANGE2=0;
		int RRANGE1=0;
		int RRANGE2=0;



		int[] HH=new int []{0,15000,20000,25000,30000,35000,40000,45000,50000,60000,75000,10000,125000,150000,175000,200000,250000,300000,400000,500000,1000000};
		int[] RR=new int[] {0,100,150,200,250,300,350,400,450,500,550,600,650,700,750,1000,1000000};


		HRANGE1=HH[H];

		HRANGE2=HH[H+1]-1;

		RRANGE1=RR[R];

		RRANGE2=RR[R+1]-1;

		Text out5=new Text("Median Value Occupied by owners is between ("+HRANGE1+" - :"+HRANGE2+") US $");	
		//context.write(key, out5);
		Text out6=new Text("Median Rent  is between ("+RRANGE1+" - "+RRANGE2+")US $");
		//context.write(key, out6);
		
		
		////NEW///
		System.out.println("TOTAL H:"+totalValues+" R:"+totalRent);	
		
		
		int MedianH1=0;
		Long medianHValue1=(long) 0;

		int MedianR1=0;
		Long medianRValue1=(long) 0;

		if(totalValues%2==0){
			MedianH1=(int) ((totalValues/2 )+1);
		}
		else{
			MedianH1=(int) (totalValues/2 );
		}

		if(totalRent%2==0){
			MedianR1=(int) ((totalRent/2 )+1);
		}
		else{
			MedianR1=(int) (totalRent/2 );
		}

		/*
		System.out.println("Median H:"+MedianH1+" R:"+MedianR1);
		
		int searchI=Arrays.binarySearch(HH, MedianH1);
		System.out.println("Insertion point:"+searchI);
		
		if(searchI<0){
			System.out.println("Insertion point:"+(-searchI-2)+"-"+((-searchI)-1));
			HRANGE1=HH[(-searchI-2)];
			HRANGE2=HH[(-searchI)-1];
		}
		else{
				System.out.println("Insertion point:("+(searchI)+"-"+(searchI+1));
				HRANGE1=HH[searchI];
				HRANGE2=HH[(searchI+1)];
		}
		
		
		
		int searchIR=Arrays.binarySearch(RR, MedianR1);
		
		System.out.println("Insertion point:"+searchIR);
		
		
		if(searchI<0){
			System.out.println("Insertion point:"+(-searchIR-2)+"-"+((-searchIR)-1));
			RRANGE1=RR[(-searchIR-2)];
			RRANGE2=RR[(-searchIR)-1];
		}
		else{
				System.out.println("Insertion point:("+(searchI)+"-"+(searchI+1));
				RRANGE1=RR[searchIR];
				RRANGE2=RR[(searchIR+1)];
		}
		
		
		int hi=0;
		int hk1=0;
		while(MedianH1>HH[hi]){
			
			hi++;
			hk1++;
		}
		
		int HRANGE11=0;
		int HRANGE12=0;
		int RRANGE11=0;
		int RRANGE12=0;
		
		HRANGE11=HH[hk1];
		HRANGE12=HH[hk1+1];
		/*
		
		int hr=0;
		int rk1=0;
		while(MedianR1>RR[hr]){
			
			hr++;
			rk1++;
		}
		
		RRANGE11=RR[rk1];
		RRANGE12=RR[rk1+1];
		*/
		//String extra=(HRANGE11+" "+HRANGE12+" "+RRANGE11+" "+RRANGE12)+"#"+(HRANGE1+" "+HRANGE2+" "+RRANGE1+" "+RRANGE2);
		
		//RRANGE1=RR[searchIR-1];
		//RRANGE2=RR[searchIR];
		
		//Output the values
		Text output=new Text(twoDec.format(ownPer)+"#"+twoDec.format(rentPer)+"#"+twoDec.format(malesPer)+"#"+twoDec.format(femalesPer)+"#"+twoDec.format(male18Per)+"#"+
				twoDec.format(female18Per)+"#"+twoDec.format(male29Per)+"#"+twoDec.format(female29Per)+"#"+twoDec.format(male39Per)+"#"+twoDec.format(female39Per)+"#"+
				twoDec.format(urbanPer)+"#"+twoDec.format(ruralPer)+"#"+HRANGE1+"#"+HRANGE2+"#"+RRANGE1+"#"+RRANGE2+"#"+totalrooms+"#"+twoDec.format(people85Per));
		context.write(key,output);

	}
}


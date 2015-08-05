package cs455.hadoop.census;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class CensusDataExtract implements Writable{

	//unique for records ... present in both the segments
	private IntWritable summaryLevel;
	private Text state;
	private LongWritable logicalRecord;
	private IntWritable part;

	//already only total two parts are present for each record

	// only in first segment
	//Marital Status
	private LongWritable maleNeverMarried;
	private LongWritable totalMales;
	private LongWritable femaleNeverMarried;
	private LongWritable totalFemales;

	//Ages
	private LongWritable malesAge;
	private LongWritable femalesAge;
	private LongWritable malesAge18;
	private LongWritable malesAge29;
	private LongWritable malesAge39;
	private LongWritable malesAge85;
	private LongWritable femalesAge18;
	private LongWritable femalesAge29;
	private LongWritable femalesAge39;
	private LongWritable femalesAge85;


	//only in second segment
	//Residences
	private LongWritable owner;
	private LongWritable renter;

	//Urban Vs Rural
	private LongWritable urban;
	private LongWritable rural;
	private LongWritable houses;

	//House Values
	private Text valueList ;

	//House Rent
	private Text rentList ;

	//House Rooms
	private Text RoomsList;

	//Constructor
	public CensusDataExtract(){
		this.summaryLevel=new IntWritable();
		this.state=new Text();
		this.logicalRecord=new LongWritable();
		this.part=new IntWritable();

		this.owner=new LongWritable();
		this.renter=new LongWritable();

		this.maleNeverMarried=new LongWritable();
		this.totalMales=new LongWritable();
		this.femaleNeverMarried=new LongWritable();
		this.totalFemales=new LongWritable();

		this.urban=new LongWritable();
		this.rural=new LongWritable();
		this.houses=new LongWritable();

		this.malesAge=new LongWritable();
		this.femalesAge=new LongWritable();
		this.malesAge18=new LongWritable();
		this.malesAge29=new LongWritable();
		this.malesAge39=new LongWritable();
		this.malesAge85=new LongWritable();
		this.femalesAge18=new LongWritable();
		this.femalesAge29=new LongWritable();
		this.femalesAge39=new LongWritable();
		this.femalesAge85=new LongWritable();


		this.valueList= new Text();
		this.rentList= new Text();
		this.RoomsList=new Text();
	}

	//### For Testing constructor
	public CensusDataExtract(IntWritable sLevel, Text st, LongWritable logRec, IntWritable part){
		this.summaryLevel=sLevel;
		this.state=st;
		this.logicalRecord=logRec;
		this.part=part;
	}
	public Text getRoomsList() {
		return RoomsList;
	}
	public void setRoomsList(Text roomsList) {
		RoomsList = roomsList;
	}
	public LongWritable getMalesAge85() {
		return malesAge85;
	}
	public void setMalesAge85(LongWritable malesAge85) {
		this.malesAge85 = malesAge85;
	}
	public LongWritable getFemalesAge85() {
		return femalesAge85;
	}
	public void setFemalesAge85(LongWritable femalesAge85) {
		this.femalesAge85 = femalesAge85;
	}

	public LongWritable getUrban() {
		return urban;
	}
	public void setUrban(LongWritable urban) {
		this.urban = urban;
	}
	public LongWritable getRural() {
		return rural;
	}
	public void setRural(LongWritable rural) {
		this.rural = rural;
	}
	public LongWritable getHouses() {
		return houses;
	}
	public void setHouses(LongWritable houses) {
		this.houses = houses;
	}
	public LongWritable getMalesAge() {
		return malesAge;
	}
	public void setMalesAge(LongWritable malesAge) {
		this.malesAge = malesAge;
	}
	public LongWritable getFemalesAge() {
		return femalesAge;
	}
	public void setFemalesAge(LongWritable femalesAge) {
		this.femalesAge = femalesAge;
	}
	public LongWritable getMalesAge18() {
		return malesAge18;
	}
	public void setMalesAge18(LongWritable malesAge18) {
		this.malesAge18 = malesAge18;
	}
	public LongWritable getMalesAge29() {
		return malesAge29;
	}
	public void setMalesAge29(LongWritable malesAge29) {
		this.malesAge29 = malesAge29;
	}
	public LongWritable getMalesAge39() {
		return malesAge39;
	}
	public void setMalesAge39(LongWritable malesAge39) {
		this.malesAge39 = malesAge39;
	}
	public LongWritable getFemalesAge18() {
		return femalesAge18;
	}
	public void setFemalesAge18(LongWritable femalesAge18) {
		this.femalesAge18 = femalesAge18;
	}
	public LongWritable getFemalesAge29() {
		return femalesAge29;
	}
	public void setFemalesAge29(LongWritable femalesAge29) {
		this.femalesAge29 = femalesAge29;
	}
	public LongWritable getFemalesAge39() {
		return femalesAge39;
	}
	public void setFemalesAge39(LongWritable femalesAge39) {
		this.femalesAge39 = femalesAge39;
	}
	public LongWritable getMaleNeverMarried() {
		return maleNeverMarried;
	}
	public void setMaleNeverMarried(LongWritable maleNeverMarried) {
		this.maleNeverMarried = maleNeverMarried;
	}
	public LongWritable getTotalMales() {
		return totalMales;
	}
	public void setTotalMales(LongWritable totalMales) {
		this.totalMales = totalMales;
	}
	public LongWritable getFemaleNeverMarried() {
		return femaleNeverMarried;
	}
	public void setFemaleNeverMarried(LongWritable femaleNeverMarried) {
		this.femaleNeverMarried = femaleNeverMarried;
	}
	public LongWritable getTotalFemales() {
		return totalFemales;
	}
	public void setTotalFemales(LongWritable totalFemales) {
		this.totalFemales = totalFemales;
	}
	public LongWritable getOwner() {
		return owner;
	}
	public void setOwner(LongWritable owner) {
		this.owner = owner;
	}
	public LongWritable getRenter() {
		return renter;
	}
	public void setRenter(LongWritable renter) {
		this.renter = renter;
	}

	public IntWritable getSummaryLevel() {
		return summaryLevel;
	}
	public void setSummaryLevel(IntWritable summaryLevel) {
		this.summaryLevel = summaryLevel;
	}
	public Text getState() {
		return state;
	}
	public void setState(Text state) {
		this.state = state;
	}
	public LongWritable getLogicalRecord() {
		return logicalRecord;
	}
	public IntWritable getPart() {
		return part;
	}
	public void setPart(IntWritable part) {
		this.part = part;
	}
	public void setLogicalRecord(LongWritable logicalRecord) {
		this.logicalRecord = logicalRecord;
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		summaryLevel.write(dataOutput);
		state.write(dataOutput);
		logicalRecord.write(dataOutput);
		part.write(dataOutput);

		owner.write(dataOutput);
		renter.write(dataOutput);

		maleNeverMarried.write(dataOutput);
		totalMales.write(dataOutput);
		femaleNeverMarried.write(dataOutput);
		totalFemales.write(dataOutput);

		urban.write(dataOutput);
		rural.write(dataOutput);
		houses.write(dataOutput);

		malesAge.write(dataOutput);
		femalesAge.write(dataOutput);
		malesAge18.write(dataOutput);
		malesAge29.write(dataOutput);		
		malesAge39.write(dataOutput);
		malesAge85.write(dataOutput);
		femalesAge18.write(dataOutput);
		femalesAge29.write(dataOutput);
		femalesAge39.write(dataOutput);
		femalesAge85.write(dataOutput);

		valueList.write(dataOutput);
		rentList.write(dataOutput);

		RoomsList.write(dataOutput);

		//Tried to implementt arraylist for writable
		/*for (LongWritable item : valueList)
            item.write(dataOutput);
		/*
		IntWritable size = new IntWritable(valueList.size());
		size.write(dataOutput);
		 for (LongWritable item : valueList)
	            item.write(dataOutput);
		 */
		/*
			IntWritable size2 = new IntWritable(rentList.size());
			size2.write(dataOutput);
			 for (LongWritable item : rentList)
		            item.write(dataOutput);
		 */ 



	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		summaryLevel.readFields(dataInput);
		state.readFields(dataInput);
		logicalRecord.readFields(dataInput);
		part.readFields(dataInput);

		owner.readFields(dataInput);
		renter.readFields(dataInput);

		maleNeverMarried.readFields(dataInput);
		totalMales.readFields(dataInput);
		femaleNeverMarried.readFields(dataInput);
		totalFemales.readFields(dataInput);

		urban.readFields(dataInput);
		rural.readFields(dataInput);
		houses.readFields(dataInput);

		malesAge.readFields(dataInput);
		femalesAge.readFields(dataInput);
		malesAge18.readFields(dataInput);
		malesAge29.readFields(dataInput);
		malesAge39.readFields(dataInput);
		malesAge85.readFields(dataInput);
		femalesAge18.readFields(dataInput);
		femalesAge29.readFields(dataInput);
		femalesAge39.readFields(dataInput);
		femalesAge85.readFields(dataInput);

		valueList.readFields(dataInput);
		rentList.readFields(dataInput);

		RoomsList.readFields(dataInput);


		//Failed to send data as arraylist from mapper to reucer..coz reducer doesnt support
		/*for (LongWritable item : valueList)
            item.readFields(dataInput);

		/*
		IntWritable size = new IntWritable();

	    size.readFields(dataInput);
	    int n = size.get();
        while(n-- > 0) {
            LongWritable item = new LongWritable();
            item.readFields(dataInput);
            valueList.add(item);

        }*/
		/*
        IntWritable size2 = new IntWritable();
	    size2.readFields(dataInput);
	    int n2 = size.get();
        while(n2-- > 0) {
            LongWritable item = new LongWritable();
            item.readFields(dataInput);
            rentList.add(item);
        }

		 */

	}

	public Text getValueList() {
		return valueList;
	}
	public void setValueList(Text valueList) {
		this.valueList = valueList;
	}
	public Text getRentList() {
		return rentList;
	}
	public void setRentList(Text rentList) {
		this.rentList = rentList;
	}

	//### For Testing
	public String toString() {
		StringBuilder str=new StringBuilder();
		str.append(this.state+" "+this.summaryLevel+" "+ this.logicalRecord+ " "+this.malesAge+" "+this.femalesAge+" "+this.malesAge18+ " "+this.femalesAge18);
		/*for(Object item: this.valueList){
			str.append((Long)item);
		}*/

		return str.toString();

	}


}

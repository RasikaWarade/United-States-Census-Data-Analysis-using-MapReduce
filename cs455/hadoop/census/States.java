package cs455.hadoop.census;

import java.util.ArrayList;
import java.util.HashMap;

public class States {

	HashMap<String, Integer> states = new HashMap<String, Integer>();
	ArrayList<Long> HouseValues = new ArrayList<Long>();
	ArrayList<Long> RentValues = new ArrayList<Long>();

	public States(){
		this.states.put("AL", 1);
		this.states.put("AK", 2);
		this.states.put("AZ", 3);
		this.states.put("AR", 4);
		this.states.put("CA", 5);
		this.states.put("CO", 6);
		this.states.put("CT", 7);
		this.states.put("DE", 8);
		this.states.put("FL", 9);
		this.states.put("GA", 10);
		this.states.put("HI", 11);
		this.states.put("ID", 12);
		this.states.put("IL", 13);
		this.states.put("IN", 14);
		this.states.put("IA", 15);
		this.states.put("KS", 16);
		this.states.put("KY", 17);
		this.states.put("LA", 18);
		this.states.put("ME", 19);
		this.states.put("MD", 20);
		this.states.put("MA", 21);
		this.states.put("MI", 22);
		this.states.put("MN", 23);
		this.states.put("MS", 24);
		this.states.put("MO", 25);
		this.states.put("MT", 26);
		this.states.put("NE", 27);
		this.states.put("NV", 28);
		this.states.put("NH", 29);
		this.states.put("NJ", 30);
		this.states.put("NM", 31);
		this.states.put("NY", 32);
		this.states.put("NC", 33);
		this.states.put("ND", 34);
		this.states.put("OH", 35);
		this.states.put("OK", 36);
		this.states.put("OR", 37);
		this.states.put("PA", 38);
		this.states.put("RI", 39);
		this.states.put("SC", 40);
		this.states.put("SD", 41);
		this.states.put("TN", 42);
		this.states.put("TX", 43);
		this.states.put("UT", 44);
		this.states.put("VT", 45);
		this.states.put("VA", 46);
		this.states.put("WA", 47);
		this.states.put("WV", 48);
		this.states.put("WI", 49);
		this.states.put("WY", 50);
		this.states.put("VI", 51);
		this.states.put("DC", 52);



		this.HouseValues.add((long) 0);
		this.HouseValues.add((long) 15000);
		this.HouseValues.add((long) 20000);
		this.HouseValues.add((long) 25000);
		this.HouseValues.add((long) 30000);
		this.HouseValues.add((long) 35000);
		this.HouseValues.add((long)20000);
		this.HouseValues.add((long)25000);
		this.HouseValues.add((long)30000);
		this.HouseValues.add((long)35000);
		this.HouseValues.add((long)40000);
		this.HouseValues.add((long)45000);
		this.HouseValues.add((long)50000);
		this.HouseValues.add((long)60000);
		this.HouseValues.add((long)75000);
		this.HouseValues.add((long)100000);
		this.HouseValues.add((long)125000);
		this.HouseValues.add((long)150000);
		this.HouseValues.add((long)175000);
		this.HouseValues.add((long)200000);
		this.HouseValues.add((long)250000);
		this.HouseValues.add((long)300000);
		this.HouseValues.add((long)400000);
		this.HouseValues.add((long)500000);
		
		
		this.RentValues.add((long)0);
		this.RentValues.add((long)100);
		this.RentValues.add((long)150);
		this.RentValues.add((long)200);
		this.RentValues.add((long)250);
		this.RentValues.add((long)300);
		this.RentValues.add((long)350);
		this.RentValues.add((long)400);
		this.RentValues.add((long)450);
		this.RentValues.add((long)500);
		this.RentValues.add((long)550);
		this.RentValues.add((long)600);
		this.RentValues.add((long)650);
		this.RentValues.add((long)700);
		this.RentValues.add((long)750);
		this.RentValues.add((long)1000);

	}
	public HashMap<String, Integer> getStates() {
		return states;
	}
	public void setStates(HashMap<String, Integer> states) {
		this.states = states;
	}

	public HashMap<String, Integer> getList(){

		return this.states;

	}
	public ArrayList<Long> getHouseValues() {
		return HouseValues;
	}
	public void setHouseValues(ArrayList<Long> houseValues) {
		HouseValues = houseValues;
	}
	public ArrayList<Long> getRentValues() {
		return RentValues;
	}
	public void setRentValues(ArrayList<Long> rentValues) {
		RentValues = rentValues;
	}
}

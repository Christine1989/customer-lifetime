package com.shutterfly.data.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.DoubleStream;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class DataAnalysis {

	public static void main(String[] args) throws Exception {
		
		HashMap<String, Double> map = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader("/Users/christine/Documents/workspace/customer-lifetime/input/input.txt"));
		String line;
		while((line = br.readLine()) != null){
			map = Ingest(line, map);
		}
		br.close();
		JsonObject report = TopSimpleLTVCustomers(2, map);
		File file = new File("/Users/christine/Documents/workspace/customer-lifetime/output/output.txt");
		FileWriter fr = new FileWriter(file);
		fr.write(report.toString());
		System.out.println(report.toString());
		fr.close();
		
	}
	
	public static HashMap<String, Double> Ingest(String input, HashMap<String, Double> map) throws Exception{
		Gson gson = new Gson();
		JsonObject[] actions = gson.fromJson(input, JsonObject[].class);
		JsonObject json = new JsonObject();
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		Date date1 = dateFormat.parse("05/01/2017");//Also can add time range as parameters, set from main function
		Date date2 = dateFormat.parse("12/01/2017");
		Timestamp timestamp1 = new Timestamp(date1.getTime());
		Timestamp timestamp2 = new Timestamp(date2.getTime());
		for(int i = 0; i < actions.length; i++){
			json = actions[i].getAsJsonObject();
			Timestamp ts = Timestamp.valueOf(json.get("event_time").getAsString().replace("T", " ").replace("Z", ""));
			//Only process the input events within this week time range which set above.
			if(ts.after(timestamp2) || ts.before(timestamp1)){
				continue;
			}
			String id = "";
			double amount = 0.0;
			if(json.get("type").getAsString().equalsIgnoreCase("CUSTOMER")){
				id = json.get("key").getAsString();
				if(!map.containsKey(id)){
					map.put(id, 0.0);
				}
			}else if(json.get("type").getAsString().equalsIgnoreCase("ORDER") && json.get("verb").getAsString().equalsIgnoreCase("NEW")){
				id = json.get("customer_id").getAsString();
				amount = Double.valueOf(json.get("total_amount").getAsString().replaceAll("[^0-9.]", ""));
				amount += (double)map.get(id);//Use this total amount as the value per week.
				map.replace(id, amount);
			}
		}
		return map;
	}
	
	public static JsonObject TopSimpleLTVCustomers(int x, HashMap<String, Double> map) {
		JsonObject json = new JsonObject();
		double[] amounts = new double[x];
		Object[] values = map.values().toArray();
		for(int n = 0; n < values.length; n++){
			if(Double.valueOf(values[n].toString()) > amounts[0]){
				amounts[0] = Double.valueOf(values[n].toString());
				Arrays.sort(amounts);
			}
		}
		Set<Entry<String, Double>> set = map.entrySet();
		Iterator<Entry<String, Double>> i = set.iterator();
		while(i.hasNext()){
			Map.Entry<String, Double> entry = i.next();
			if(DoubleStream.of(amounts).anyMatch(k -> k == entry.getValue())){
				double lifetime_value = 52 * (double)entry.getValue() * 10;
				json.addProperty(entry.getKey().toString(), lifetime_value);//return customer_id and lifetime_value pairs
			}
		}
		return json;
	}

}

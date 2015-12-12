package org.kmeans.bisec;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * each row of the features vector
 * is hashmap where the key is the name of the
 * word and the value a inverse frequency of it
 * 
 */

public class Document {
	
	public Document() {
		
	}
	
	private String name="centroid";
	
	private HashMap<String, Double> words = new HashMap<String, Double>();
	
	//in the centroid no take the column 0
	public Document(String line,int what) {
		//separate for coloumn
		String[] columns = line.split(";");
		
		//the name of the document
		if(what==1)
			this.name="centroid";
		if(what==2)
			this.name = columns[0];
		
		for(int i = 1; i < columns.length; i++) {
			//separate key and value
			String[] keyAndValue = columns[i].split(":");
			
			
			try{
				if(keyAndValue.length == 1)
				{
					System.out.println(columns[i]);
					throw new Exception("e");
				}
				this.words.put(keyAndValue[0], Double.parseDouble(keyAndValue[1]));
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
		}
		
		
	}
	

	public String getName(){
		return name;
	}
	
	public HashMap<String, Double> getMap(){
		return words;
	}
	
	public String toString(){
		
		StringBuilder toReturn=new StringBuilder();
		
		toReturn.append(name);
		
		for(Entry<String,Double> entry:words.entrySet()){
			toReturn.append(entry.getKey()+":"+entry.getValue());
		}
		
		return toReturn.toString();
		
	}
	
	
}

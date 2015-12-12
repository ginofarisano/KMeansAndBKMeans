package org.kmeans.bisec;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

/*
 * contains the document and the centroids
 */
public class Cluster {
	
	private static final String FILE="File";
	private static int  counter=0;
	
	private static int topX;
	

	public Cluster(){
		try {
			counter++;
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(fileName=FILE+counter+".txt"), "utf-8"));
					//new FileOutputStream(fileName=randomName.nextDouble()+".txt"), "utf-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	//for the list of documents i usu a file

	//ArrayList<Document> documents = new ArrayList<Document>();

	String fileName;

	Writer writer = null;

	BufferedReader inData=null;

	Random randomName=new Random();

	int size=0;


	Document centroid;

	public void addDocument(String row) {
		//		documents.add(document);
		try {
			
			writer.append(row+'\n');
			size++;

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void endAddDocument() {

		try {
			writer.flush();
			writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}



	public void eraseDocument(){
		try {
			
			size=0;
			
			writer=new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(fileName), "utf-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public int size() {
		return size;
	}

	public BufferedReader getDocuments(){
		try {
			inData = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return inData;
	}

	public void setCentroids(Document document) {
		this.centroid = document;
	}

	public Document getCentroid(){
		return centroid;
	}

	//use this method for cut a cluster in two
	public Document[] getTwoRandomDocument() throws IOException{
		
		Document[] toReturn = new Document[2];

		Random random=new Random();


		int firstRandom=random.nextInt(size-1);
		int secondRandom=random.nextInt(size-1);
		//the second random number must be different for the first
		while(firstRandom==secondRandom)
			secondRandom=random.nextInt(size-1);

		//open a file in read
		
		System.out.println("The centroids calculated are "+firstRandom+" and "+secondRandom);
		String line, line1 = null,line2 = null;
		
		int i=0;
		
		inData=new BufferedReader(new FileReader(fileName));

		while((line=inData.readLine())!=null){		
			
			if(i==firstRandom)
				line1=line;
			
			if(i==secondRandom)
				line2=line;
			
			if(line1 != null && line2 != null)
				break;
				
			i++;
		}
		inData.close();
		toReturn[0]=new Document(line1, 2);
		toReturn[1]=new Document(line2, 2);

		return toReturn;

	}

	public void calculateCentroid(int topX) throws IOException {

		
		Document centroidTemp=new Document();

		try {
			inData = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		String line;

		Document document;

		while((line=inData.readLine())!=null){		

			document=new Document(line,2);

			Iterator<Entry<String, Double>> iter = document.getMap()
					.entrySet().iterator();

			while (iter.hasNext()) {
				Entry<String, Double> word = iter.next();
				if (centroidTemp.getMap().containsKey(word.getKey()))
					centroidTemp.getMap().put(
							word.getKey(),
							centroidTemp.getMap().get(word.getKey())
							+ (word.getValue() / (size)));
				else
					centroidTemp.getMap().put(word.getKey(),
							word.getValue() / size);
			}

		}

		inData.close();
		
		Double lowest;
		
		TreeMap<Double, String > sortedMap = new TreeMap<Double, String>();
		
		for (Map.Entry<String, Double> entry : centroidTemp.getMap().entrySet()) {
			
			if(sortedMap.size()<topX)
				sortedMap.put((Double) entry.getValue(), (String)entry.getKey());
			else {
				
				lowest = sortedMap.firstKey();
				
				if(entry.getValue() > lowest){
					sortedMap.pollFirstEntry();
					sortedMap.put(entry.getValue(),entry.getKey());
				}
				
				
			}
		}
		
		//assign the topX element of the sortedMap at the centroid
		
		centroid = new Document();
		
		Entry<Double, String> greatest;
		
		for(int i=0;i<topX;i++){
			
			greatest = sortedMap.pollLastEntry();
			
			if(greatest==null)
				break;
			
			centroid.getMap().put(greatest.getValue(), greatest.getKey());
			
		}
		
		sortedMap=null;
			

	}


	public String print(int clusters) throws IOException{

		String temp;
		HashMap<String, Integer> typeOfDocuments = new HashMap<String, Integer>();

		//if you would also the words in the document decomments this code	
		//		StringBuilder toReturn2=new StringBuilder();

		try {
			inData = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
						e.printStackTrace();
		}

		String line;

		Document document;

		while((line=inData.readLine())!=null){	

			document=new Document(line,2);
			//if you would also the words in the document decomments this code
			//			toReturn2.append(document);

			//insert here the delimiter of the category
			temp=document.getName();
			
			/************************/
			
			//change this code for real experiment
			
			/************************/
			
			String result = temp.substring(temp.indexOf("@") + 1, temp.indexOf("-"));
			
			
			if(typeOfDocuments.containsKey(result)){
				int toIncrement=typeOfDocuments.get(result);
				typeOfDocuments.put(result, toIncrement+1);
			} else
				typeOfDocuments.put(result, 1);
		}

		StringBuilder toReturn1=new StringBuilder();

		toReturn1.append("\n\nThe cluster contains "+size()+" documents");

		for(Entry<String,Integer> entry: typeOfDocuments.entrySet()){
			toReturn1.append("\nDocument of type "+entry.getKey()+" are "+entry.getValue());
		}

		//if you would also the words in the document decomments this code
		//toReturn1.append(toReturn2);
		
		toReturn1.append("\nCentroid: "+centroid+"\n\n_______________________________________________\n");

		return toReturn1.toString();

	}

	public void removeFile() {
		
		File file = new File(fileName);
		 
		file.delete();

		
	}

}

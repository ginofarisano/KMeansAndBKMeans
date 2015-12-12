package org.kmeans.bisec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
/*
 * contains the document and the centroids
 */
public class Cluster {
	
	ArrayList<Document> documents = new ArrayList<Document>();
	Document centroid;
	
	public void addDocument(Document document) {
		documents.add(document);
	}
	
	public void eraseDocument(){
		documents=new ArrayList<Document>();
	}
	
	public int size() {
		return documents.size();
	}
	
	public ArrayList<Document> getDocuments(){
		return documents;
	}
	
	public void setCentroids(Document document) {
		this.centroid = document;
	}
	
	public Document getCentroid(){
		return centroid;
	}
	
	//use this method for cut a cluster in two
	public Document[] getTwoRandomDocument(){
		Document[] toReturn = new Document[2];
		
		Random random=new Random();
		int size=documents.size();
		
		int firstRandom=random.nextInt(size);
		int secondRandom=random.nextInt(size);
		//the second random number must be different for the first
		while(firstRandom==secondRandom)
			secondRandom=random.nextInt(size);
		
		toReturn[0]=documents.get(firstRandom);
		toReturn[1]=documents.get(secondRandom);
		
		return toReturn;
		
	}
	
	public void calculateCentroid() {
		centroid = new Document();
		for (int i = 0; i < documents.size(); i++) {
			Iterator<Entry<String, Double>> iter = documents.get(i).getMap()
					.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Double> word = iter.next();
				if (centroid.getMap().containsKey(word.getKey()))
					centroid.getMap().put(
							word.getKey(),
							centroid.getMap().get(word.getKey())
									+ (word.getValue() / (documents.size())));
				else
					centroid.getMap().put(word.getKey(),
							word.getValue() / documents.size());
			}
		}
	}
	
	
	public String print(int clusters){
		
		String[] temp;
		HashMap<String, Integer> typeOfDocuments = new HashMap<String, Integer>();
		
		//if you would also the words in the document decomments this code	
		//StringBuilder toReturn2=new StringBuilder();
		
		StringBuilder documentList=new StringBuilder();
		
		documentList.append("The document in the list are:\n");
		
		for(Document document: documents){
			
			
			documentList.append(document.getName()+"\n");
			
			//if you would also the words in the document decomments this code
//			toReturn2.append(document);
			
			//insert here the delimiter of the category
			temp=document.getName().split("#");
			if(typeOfDocuments.containsKey(temp[0])){
				int toIncrement=typeOfDocuments.get(temp[0]);
				typeOfDocuments.put(temp[0], toIncrement+1);
			} else
				typeOfDocuments.put(temp[0], 1);

		}
		
		StringBuilder toReturn1=new StringBuilder();
		
		toReturn1.append(documentList);
		
		toReturn1.append("\nThen the cluster contains "+size()+" documents");
		
		for(Entry<String,Integer> entry: typeOfDocuments.entrySet()){
			toReturn1.append("\nDocument of type "+entry.getKey()+" are "+entry.getValue());
		}
		
		//if you would also the words in the document decomments this code
		//toReturn1.append(toReturn2);
		
		toReturn1.append("\nCentroid: "+centroid+"\n\n_______________________________________________\n");
		
		return toReturn1.toString();
		
	}
	
}

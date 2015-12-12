package org.kmeans.bisec;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.kmeans.distances.DiscanceStrategy;

public class Bisec {

	ArrayList<Cluster> clusters = new ArrayList<Cluster>();
	int numberOfIterations;
	private DiscanceStrategy distance;

	public void firstStep(String datafile,int iterations, DiscanceStrategy distance) throws IOException {
		
		this.distance=distance;
		numberOfIterations=iterations;
		BufferedReader inData = null;
		inData = new BufferedReader(new FileReader(datafile));
		String row;

		//at the beginning there is a single large cluster
		Cluster firstCluster=new Cluster();

		while ((row = inData.readLine()) != null) {
			//features vector
			firstCluster.addDocument(new Document(row,2));
		}
		clusters.add(firstCluster);
	}

	public void otherStep() {
		// iter the cluster
		// what is the major
		int maxCluster=maxCluster();
		//two new centroid
		//two document random

		Document[] toReturn;
		toReturn=clusters.get(maxCluster).getTwoRandomDocument();
		Cluster c1=new Cluster();
		Cluster c2=new Cluster();
		c1.setCentroids(toReturn[0]);
		c2.setCentroids(toReturn[1]);

		double d1;
		double d2;


		ArrayList<Document> documents=clusters.get(maxCluster).getDocuments();

		for(int i=0;i<numberOfIterations;i++){
			
			System.out.println("Iteration "+i);
			
			for(Document document : documents){
				d1 = distance.distance(document, c1.getCentroid());
				d2 = distance.distance(document, c2.getCentroid());
				
				if (d1 < d2)
					c1.addDocument(document);
				else
					c2.addDocument(document);
			}
			c1.calculateCentroid();
			c2.calculateCentroid();
			
			if(i!=numberOfIterations-1){
				c1.eraseDocument();
				c2.eraseDocument();
			}
			

		}

		//remove the hold cluster
		//now are two
		clusters.remove(maxCluster);	
		clusters.add(c1);
		clusters.add(c2);

	}



	public int maxCluster() {
		int max=clusters.get(0).size();
		int indexMax=0;
		int temp;
		for (int i = 1; i < clusters.size(); i++) {
			temp=clusters.get(i).size();
			if(temp>max){
				max=temp;
				indexMax=i;
			}
		}
		return indexMax;
	}

	public void test(int numberOfClusters) {
		Writer writer = null;

		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("output.txt"), "utf-8"));
			writer.write("The number of clusters are: "+numberOfClusters+"\n_______________________________________________\n\n");
			
			for(Cluster cluster: clusters){
				writer.write(cluster.print(numberOfClusters));
			}
			
			
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}
}

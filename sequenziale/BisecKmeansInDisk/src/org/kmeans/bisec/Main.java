package org.kmeans.bisec;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.kmeans.distances.CosiceDistance;
import org.kmeans.distances.DiscanceStrategy;
import org.kmeans.distances.EuclideanDistance;

public class Main {

	public static void main(String[] args) throws IOException {

		BufferedReader myConsole = new BufferedReader(new InputStreamReader(System.in));
		Bisec bisec=new Bisec();
		int clusters=0;
		int iterations = 0;
		String fileName=null;
		System.out.println ("Enter the number of clusters: ");

		try {
			clusters = Integer.parseInt (myConsole.readLine ());
		} catch (Exception e) {
			System.out.println ("Problem reading the number of centroids");
		}

		System.out.println ("Enter the file with the documents: ");
		try {
			fileName = myConsole.readLine ();
		} catch (Exception e) {
			System.out.println ("Problem reading the name of the file");
		}


		System.out.println ("Enter the numbers of iterations: ");
		try {
			iterations = Integer.parseInt(myConsole.readLine ());
		} catch (Exception e) {
			System.out.println ("Problem reading numbers of iterations");
		}
		
		DiscanceStrategy strategy = null;
		
		System.out.println ("Enter 1 for use the euclidean distance and 2 for the cosine distance: ");
		try {
			int choice = Integer.parseInt(myConsole.readLine ());
			
			if(choice==1)
				strategy=new EuclideanDistance();
			else
				strategy=new CosiceDistance();
			
		} catch (Exception e) {
			System.out.println ("Problem reading numbers of iterations");
		}
		
		int numbersOfDocuments = 0;
		
		System.out.println ("Enter the numbers of documents: ");
		try {
			
			numbersOfDocuments = Integer.parseInt(myConsole.readLine ());
			
		} catch (Exception e) {
			System.out.println ("Problem reading numbers of documents");
		}

		//read the file with the features
		bisec.firstStep(fileName,iterations,strategy,numbersOfDocuments);

		//other bisec
		for(int i=1;i<clusters;i++) {
			System.out.println("Round: " + i);
				bisec.otherStep(i);
		}
		
		bisec.test(clusters);
		
		System.out.println ("Finish");
		
	}
	
	
	
	
}


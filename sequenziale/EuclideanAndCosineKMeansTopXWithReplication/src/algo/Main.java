package algo;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;


public class Main {
	
	public static final String CENTROIDS="centroids";
	public static final String SOLUTION="solution.txt";
	
	
	//probability when select centroids
	static  int probability=100;
	static int numberOfDocuments=0;

	static  int iterations = 3;
	
	static  int numbersOfClusters = 20;
	
	
	//(8,2*48,2)/5,2
	//((8,2*48,2)/5,2)/8,2=9,2
	static int iterFile=9;
	
	public static  String filename = "TF";
	
	
	
	static int topX;
	
	//1 euclidean distance, 2 cosine
	static int what=2;
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		try{
			what=Integer.parseInt(args[0]);
			numbersOfClusters=Integer.parseInt(args[1]);
			iterations=Integer.parseInt(args[2]);
			filename=args[3];
			probability=Integer.parseInt(args[4]);
			numberOfDocuments=Integer.parseInt(args[5]);
			topX=Integer.parseInt(args[6]);
			iterFile=Integer.parseInt(args[7]);
			
			
		} catch(Exception e){
			e.printStackTrace();
			System.err.println("Usage: <what>: 1 euclidean and 2 cosine");
			System.err.println("Usage: <numbersOfClusters>: an int");
			System.err.println("Usage: <iterations>: an int");
			System.err.println("Usage: <filename>: file input");
			System.err.println("Usage: <probability>: an int");
			System.err.println("Usage: <number of document>: an int");
			System.err.println("Usage: <number of element in the centroid>: an int");
			System.err.println("Usage: <iter file input>: an int");
		}
		
		
		KMeans kmeans = null;
		
		if(what==1)
			kmeans = new KMeans_euclideo();
		else if(what==2)
			kmeans = new KMeans_cosine();
		
		kmeans.setNumberOfDocument(numberOfDocuments);
		
		kmeans.generateCentroids(numbersOfClusters, filename,probability,topX,iterFile);
		
		
		
		ArrayList<Document> centroids = kmeans.run(filename, iterations);
		
		//return the classification of the documents
		
		
		//kmeans.sortDocumentByCentroids(filename, centroids);
		
		System.out.println("\nFinish ");

	}

}

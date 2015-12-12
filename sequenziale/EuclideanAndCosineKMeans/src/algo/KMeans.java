package algo;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;


public abstract class KMeans {


	ArrayList<Document> centroids=new ArrayList<Document>();
	private int numberOfDocument;

	public abstract double calculateDistance(Document d1, Document d2);


	/**
	 * write in the file clusters.txt k random features vector 
	 * @param probability 
	 */
	public void generateCentroids(int k, String dataFile, int probability){


		System.out.println("Start generate centroids");

		BufferedReader inData = null;

		Random random=new Random();

		try {

			inData = new BufferedReader (new FileReader (dataFile));

			int myrandom=random.nextInt(probability);

			int selected=0;

			String cur;

			ArrayList<String> stockCentroid=new ArrayList<String>();

			while((cur = inData.readLine ())!=null){

				if(random.nextInt(probability)==myrandom){

					centroids.add(new Document(cur));

					selected++;

					if(selected==k)
						break;
				} else{
					if(stockCentroid.size()<k)

						stockCentroid.add(cur);
				}


			}
			//if the selected line are <k i use the stock array
			if(selected<k){

				//shuffle the stock array
				Collections.shuffle(stockCentroid);

				for(int i=selected,index=0;i<k;i++,index++)
					centroids.add(new Document(stockCentroid.get(index)));

			}


			inData.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Finish generate centroids");

	}

	/**
	 * read the features vector file and
		assign each at the nearest centroid
		calculate the evarages
	 * @param dataFile
	 */
	public ArrayList<Document> run(String dataFile, int iteration) {

		try {

			int k = centroids.size();
			
			int docNumber;

			for(int iter = 0; iter < iteration; iter++) {
				
				docNumber=1;

				System.out.println("Start iteration "+iter);

				//initialize the empty centroids 
				ArrayList<Document> newCentroid = new ArrayList<Document>();

				for(int i = 0; i < k; i++) {
					newCentroid.add(new Document());
				}


				String rowVector;
				Document documentvector;

				BufferedReader inData = new BufferedReader (new FileReader (dataFile));
				//what is it the nearest centroids of the row readed?
				while((rowVector = inData.readLine()) != null){


					documentvector=new Document(rowVector);

					//System.out.println("Start process "+documentvector.getName());
					System.out.println("Start process docNumber "+docNumber+" of "+this.numberOfDocument);
					System.out.println("Iteration: "+iter);
					

					double dist=Double.POSITIVE_INFINITY;
					double temp;

					int indexClusterCloser = 0;

					for(int i = 0 ; i < k ;i++){
						temp = this.calculateDistance(centroids.get(i),documentvector);
						if(temp < dist){
							indexClusterCloser = i;
							dist = temp;
						}
					}
					newCentroid.get(indexClusterCloser).mergeDocument(documentvector);

					System.out.println("End process ");
					
					docNumber++;

				}
				inData.close();

				//recalculation centroids
				for(Document d : newCentroid) {

					d.normalize();
				}
				centroids = newCentroid;

				System.out.println("End iteration "+iter);



			}

			return centroids;

		} catch(Exception e) {
			e.printStackTrace();	
		}

		return null;
	}

	/**
	 * print the solution of the k-means...is another k-menas iteration
	 * @param dataFile
	 * @param centroids
	 * @throws UnsupportedEncodingException 
	 * @throws FileNotFoundException 
	 */
	public void sortDocumentByCentroids(String dataFile, ArrayList<Document> centroids) throws FileNotFoundException, UnsupportedEncodingException {

		Writer writerCentroids = new PrintWriter(Main.CENTROIDS, "UTF-8");

		try {

			int k = centroids.size();

			for (Document item : centroids){

				writerCentroids.append(item.getWords().toString()+"Numbers of documents: "+item.getNumOfDocs());
				writerCentroids.append("\n\n");
			}

			writerCentroids.close();

			ArrayList<Writer> writerCentroidsFile=new ArrayList<Writer>();


			for(int i = 0; i < k; i++) {
				writerCentroidsFile.add(new PrintWriter(Main.CENTROIDS+i+".txt", "UTF-8"));
			}

			String rowVector;
			Document documentvector;
			BufferedReader inData = new BufferedReader (new FileReader (dataFile));

			//find the min distance from a feature vector row and a centroid 
			while((rowVector = inData.readLine()) != null){
				documentvector = new Document(rowVector);
				double dist = Double.POSITIVE_INFINITY;
				double temp;

				int indexClusterCloser = 0;

				for(int i = 0 ; i < k ;i++){
					temp = this.calculateDistance(centroids.get(i), documentvector);
					if(temp < dist){
						indexClusterCloser = i;
						dist = temp;
					}
				}

				writerCentroidsFile.get(indexClusterCloser).append(documentvector.getName());
				writerCentroidsFile.get(indexClusterCloser).append("\n");

			}

			for(int i=0;i<k;i++){
				writerCentroidsFile.get(i).close();
			}

			inData.close();

			/*Writer writerSolution = new PrintWriter(Main.SOLUTION, "UTF-8");

			for(ArrayList<String> cluster : clusters) {

				writerSolution.append("Size of the cluster: "+ cluster.size()+"\n");

				System.out.println(cluster.size());

				writerSolution.append("Documents: "+ cluster+"\n\n\n");

				System.out.println(cluster);
			}

			writerSolution.close();	
			 */
		} catch(Exception e) {
			e.printStackTrace();
		}
	}


	public void setNumberOfDocument(int numberOfDocuments) {
		this.numberOfDocument=numberOfDocuments;
		
	}

}

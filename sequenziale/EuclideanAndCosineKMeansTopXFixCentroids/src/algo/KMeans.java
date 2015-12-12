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
	private int topX;
	
	public abstract double calculateDistance(Document d1, Document d2);

	/**
	 * write in the file clusters.txt k random features vector 
	 * @param probability 
	 * @param topX2 
	 */
	public void generateCentroids(int k, String dataFile, int probability, int topX2){
		
		this.topX= topX2;

		System.out.println("Start generate centroids");

		BufferedReader inData = null;

		Random random=new Random();

		try {

			inData = new BufferedReader (new FileReader (dataFile));

			int myrandom=random.nextInt(probability);

			int selected=0;

			String cur;

			ArrayList<String> stockCentroid=new ArrayList<String>();
			
			
			
			String a1="@Arte",a2="@Lettere",a3="@Fisica",a4="@Matematica",a5="@Biologia",a6="@Economia",a7="@Medicina",a8="@Ingegneria",a9="@Lingue";
			boolean aa1=false,aa2=false,aa3=false,aa4=false,aa5=false,aa6=false,aa7=false,aa8=false,aa9=false;
			int aaa1=0,aaa2=0,aaa3=0,aaa4=0,aaa5=0,aaa6=0,aaa7=0,aaa8=0,aaa9=0;
			while((cur = inData.readLine ())!=null){

//				if(random.nextInt(probability)==myrandom){
//
//					centroids.add(new Document(cur));
//
//					selected++;
//
//					if(selected==k)
//						break;
//				} else{
//					if(stockCentroid.size()<k)
//
//						stockCentroid.add(cur);
//				}
				
				if(cur.contains(a1) && !aa1){
					if(aaa1==3){
						centroids.add(new Document(cur));
						aa1=true;
					}
					aaa1++;
				} else if(cur.contains(a2) && !aa2){
					if(aaa2==3){
						centroids.add(new Document(cur));
						aa2=true;
					}
					aaa2++;
				} else if(cur.contains(a3) && !aa3){
					if(aaa3==3){
						centroids.add(new Document(cur));
						aa3=true;
					}
					aaa3++;
				} else if(cur.contains(a4) && !aa4){
					if(aaa4==3){
						centroids.add(new Document(cur));
						aa4=true;
					}
					aaa4++;
				} else if(cur.contains(a5) && !aa5){
					if(aaa5==3){
						centroids.add(new Document(cur));
						aa5=true;
					}
					aaa5++;
				} else if(cur.contains(a6) && !aa6){
					if(aaa6==3){
						centroids.add(new Document(cur));
						aa6=true;
					}
					aaa6++;
				} else if(cur.contains(a7) && !aa7){
					if(aaa7==3){
						centroids.add(new Document(cur));
						aa7=true;
					}
					aaa7++;
				} else if(cur.contains(a8) && !aa8){
					if(aaa8==3){
						centroids.add(new Document(cur));
						aa8=true;
					}
					aaa8++;
				} else if(cur.contains(a9) && !aa9){
					if(aaa9==3){
						centroids.add(new Document(cur));
						aa9=true;
					}
					aaa9++;
				} 


			}
//			//if the selected line are <k i use the stock array
//			if(selected<k){
//
//				//shuffle the stock array
//				Collections.shuffle(stockCentroid);
//
//				for(int i=selected,index=0;i<k;i++,index++)
//					centroids.add(new Document(stockCentroid.get(index)));
//
//			}


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

					d.normalize(topX);
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

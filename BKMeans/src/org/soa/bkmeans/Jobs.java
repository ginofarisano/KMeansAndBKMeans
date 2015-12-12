package org.soa.bkmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.soa.cenrandom.CombinerCentroids;
import org.soa.cenrandom.MapCentroids;
import org.soa.cenrandom.ReducerCentroids;


public class Jobs extends Configured implements Tool {

	private static final String RANDOM_CENTROIDS_DIR ="RND_CENTROIDS/";
	private static final String RANDOM_CENTROIDS_PATH = RANDOM_CENTROIDS_DIR + "random_centroids";
	private static final String CENTROIDS_DIR = "CENTROIDS/";
	private static final String CENTROIDS_PATH = CENTROIDS_DIR + "centroids";
	private static final String TEMP_OUTPUT = "OUTPUT_TEMP/";
	private Path inputPath ;
	private Path outputPath;
	private FileSystem fs;

	private int randomCentroid_counter = -1;
	private int centroid_counter = -1;
	private int M = 264;

	//private URI randomCentroidURI;
	//private URI centroidiURI;
	//private Path centroidePath = new Path(CENTROIDS_PATH);

	public static void main(String args[]) throws Exception {

		int res = ToolRunner.run(new Jobs(), args);
		System.exit(res);
	}



	public int run(String[] args) throws Exception {


		inputPath = new Path(args[0]);
		int numberOfCLusters=Integer.parseInt(args[1]);
		int numberOfIterations=Integer.parseInt(args[2]);

		this.fs = FileSystem.get(this.getConf());

		//cancello le dir
		outputPath = new Path(TEMP_OUTPUT);
		if(fs.exists(outputPath)) this.fs.delete(outputPath, true);

		//CANCELLO LA DIR CON I CENTROIDI PARZIALI
		Path t = new Path(RANDOM_CENTROIDS_DIR);
		if(fs.exists(t)) this.fs.delete(t, true);

		t = new Path(CENTROIDS_DIR);
		if(fs.exists(t)) this.fs.delete(t, true);

		for(int i=2;i<=numberOfCLusters;i++){

			//check centroide più grande
			int centroIndexMax = maxCentroid();

			//generiamo il centroide
			generateNewCentroids(centroIndexMax);

			//eseguiamo il kmeans
			for(int j=0;j<numberOfIterations;j++){
				kMeans(centroIndexMax, j);

				//controlla la convergenza
				if(checkConvergenza())
					break;

			}

			//correggere il file centroidi
			sostituisciCentroide(centroIndexMax);
		}

		return 0;

	}

	private int kMeans( int centroIndexMax, int j) throws Exception {

		Configuration conf;
		int succesFlag;


		conf = getConf();
		conf.setInt("index", centroIndexMax);


		//cancelliamo il vecchio output
		if(fs.exists(outputPath))
			fs.delete(outputPath, true);

		//avviamo il nuovo job
		Job job = Job.getInstance(conf, "KMeans iteration: "+j);
		FileInputFormat.setInputPaths(job, this.inputPath);
		FileOutputFormat.setOutputPath(job, this.outputPath);

		//setto la distribuited cache
		job.addCacheFile(new URI(RANDOM_CENTROIDS_PATH+"_"+this.randomCentroid_counter)); 
		if(centroIndexMax != -1)
			job.addCacheFile(new URI(CENTROIDS_PATH+"_"+this.centroid_counter));


		job.setJarByClass(this.getClass());
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DocumentWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MapKMeans.class);
		job.setCombinerClass(CombineKMeans.class);
		job.setReducerClass(ReduceKMeans.class);

		succesFlag=job.waitForCompletion(true) ? 0 : 1;

		//creo il nuovo file del centroide
		randomCentroidi_unisciFile();

		if(succesFlag==1){
			System.out.println("Problem in iteration "+j);
			return 1;
		}
		return succesFlag;
	}


	private void randomCentroidi_unisciFile() throws IOException {


		this.randomCentroid_counter++;

		FileUtil.copyMerge(this.fs, this.outputPath, this.fs, new Path(RANDOM_CENTROIDS_PATH + "_" + this.randomCentroid_counter), false, this.getConf(), null);

		//cancello l'output dir
		if(fs.exists(outputPath))
			fs.delete(outputPath, true);


	}


	private void sostituisciCentroide(int index) {

		ArrayList <String> toWrite = new ArrayList<String>();
		try {

			//leggo i centroidi random
			FSDataInputStream sr = this.fs.open(new Path(RANDOM_CENTROIDS_PATH + "_" + this.randomCentroid_counter));
			InputStreamReader isr = new InputStreamReader(sr);
			BufferedReader br = new BufferedReader(isr);

			toWrite.add(br.readLine());
			toWrite.add(br.readLine());

			br.close();
			isr.close();
			sr.close();


			//non è il primo round
			Path centroidePath = new Path(CENTROIDS_PATH + "_" + this.centroid_counter);
			if(fs.exists(centroidePath)) {

				//leggo il vecchio file dei centroidi da HDFS
				sr = this.fs.open(centroidePath);
				isr = new InputStreamReader(sr);
				br = new BufferedReader(isr);

				int i=0;
				String str;
				while( (str = br.readLine()) != null){

					if(i != index) //salto l'indice max
						toWrite.add(str);

					i++;	
				}
				br.close();
				isr.close();
				sr.close();
			}


			//riscrivo (o scrivo) il file dei centroidi
			this.centroid_counter++;
			FSDataOutputStream sw = fs.create(new Path(CENTROIDS_PATH + "_" + this.centroid_counter), true);
			OutputStreamWriter osw = new OutputStreamWriter(sw);
			BufferedWriter dw = new BufferedWriter(osw);
			for(String str: toWrite) {
				dw.write(str+"\n");	
			}
			dw.close();
			osw.close();
			sw.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private int maxCentroid() throws IOException{

		Path p = new Path(CENTROIDS_PATH + "_" + this.centroid_counter);

		//leggo i centroidi
		if(!this.fs.exists(p))
			return -1;

		FSDataInputStream sr = this.fs.open(p);
		BufferedReader br = new BufferedReader(new InputStreamReader(sr));


		String [] temp;
		int max=-1;
		int value=0;
		int i=0;
		int index=-1; //non c'è il file

		String line=br.readLine();

		while(line!=null){
			temp=line.split(";");
			value=Integer.parseInt(temp[0].trim());
			if(value>max){
				max=value;
				index=i;
			}

			i++;
			line=br.readLine();
		}

		//dico su quanti elementi andare a calcolare la probabilità
		this.M = value;

		br.close();
		sr.close();
		return index;
	}

	private boolean checkConvergenza() throws IllegalArgumentException, IOException {
		ArrayList <String> toWrite = new ArrayList<String>();


		//leggo i centroidi random
		FSDataInputStream sr = this.fs.open(new Path(RANDOM_CENTROIDS_PATH + "_" + this.randomCentroid_counter));
		InputStreamReader isr = new InputStreamReader(sr);
		BufferedReader br = new BufferedReader(isr);

		toWrite.add(br.readLine());
		toWrite.add(br.readLine());

		br.close();
		isr.close();
		sr.close();

		sr = this.fs.open(new Path(RANDOM_CENTROIDS_PATH + "_" + (this.randomCentroid_counter-1)));
		isr = new InputStreamReader(sr);
		br = new BufferedReader(isr);

		toWrite.add(br.readLine());
		toWrite.add(br.readLine());

		br.close();
		isr.close();
		sr.close();

		String v1,v2;
		v1 = toWrite.get(0); //prima riga primo file
		v2 = toWrite.get(2); //prima riga secondo file
		if(v1.equals(v2)) {
			v1 = toWrite.get(1); //seconda riga primo file
			v2 = toWrite.get(3); //seconda riga secondo file
			if(v1.equals(v2))
				return true;
		} else {
			v1 = toWrite.get(1); //seconda riga primo file
			if(v1.equals(v2)) { //prima riga primo file

				v1 = toWrite.get(0); //prima riga primo file
				v2 = toWrite.get(3);//seconda riga secondo file
				if(v1.equals(v2))
					return true;
			}
		}


		return false;
	}

	private void generateNewCentroids (int indexCentroidMax) throws Exception {

		Configuration conf = new Configuration();

		//cancelliamo il vecchio output dir
		if(fs.exists(outputPath))
			fs.delete(outputPath, true);


		conf.setInt("index", indexCentroidMax);
		conf.setInt("prob", (this.M / 20) ); // prob è 1/20
		Job jobCentroids = Job.getInstance(conf,"Calculate random centroids");

		FileInputFormat.setInputPaths(jobCentroids, this.inputPath);
		FileOutputFormat.setOutputPath(jobCentroids, this.outputPath);
		jobCentroids.setJarByClass(this.getClass());
		jobCentroids.setInputFormatClass(TextInputFormat.class);
		jobCentroids.setOutputFormatClass(TextOutputFormat.class);
		jobCentroids.setMapOutputKeyClass(LongWritable.class);
		jobCentroids.setMapOutputValueClass(Text.class);

		//?????
		//jobCentroids.setNumReduceTasks(1);

		jobCentroids.setMapperClass(MapCentroids.class);
		jobCentroids.setCombinerClass(CombinerCentroids.class);
		jobCentroids.setReducerClass(ReducerCentroids.class);

		jobCentroids.setMapOutputKeyClass(IntWritable.class);
		jobCentroids.setMapOutputValueClass(Text.class);
		jobCentroids.setOutputKeyClass(Text.class);
		jobCentroids.setOutputValueClass(Text.class);

		//non è la prima chiamata
		if(indexCentroidMax  != -1)
			jobCentroids.addCacheFile(new URI(CENTROIDS_PATH+"_"+this.centroid_counter));

		jobCentroids.waitForCompletion(true);

		randomCentroidi_unisciFile();

	}
}






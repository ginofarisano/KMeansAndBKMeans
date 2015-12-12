package algo;
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

	private String documentName;

	private HashMap<String, Double> words = new HashMap<String, Double>();
	private Integer numOfDocs = 0;

	public Document() {}

	public Document(String line) {
		//separate for coloumn
		String[] columns = line.split(";");

		//the name of the document
		this.documentName = columns[0];

		for(int i = 1; i < columns.length; i++) {
			//separate key and value
			String[] keyAndValue = columns[i].split(":");
			this.words.put(keyAndValue[0], Double.parseDouble(keyAndValue[1]));
		}

		this.numOfDocs++;
	}


	public Integer getNumOfDocs() {
		return numOfDocs;
	}

	public String getName(){
		return documentName;
	}

	public HashMap<String, Double> getWords(){
		return words;
	}

	public HashMap<String, Double> getMap(){
		return words;
	}

	public void normalize() {

		Iterator<Entry<String, Double>> iter = this.words.entrySet().iterator();

		while(iter.hasNext()) {
			Entry<String, Double> t = iter.next();
			//average
			double value = t.getValue() / this.numOfDocs;
			t.setValue(value);
		}

	}

	public void mergeDocument(Document d1)  {

		//for every word of d1
		Iterator<Entry<String, Double>> iter = d1.getMap().entrySet().iterator();
		double valore;

		while(iter.hasNext()) {

			//read word for word
			Entry<String, Double> t = iter.next();

			valore = t.getValue();

			//Has the centroid  the key of the document?
			Double t2 = (double) this.words.getOrDefault(t.getKey(), 0.0);  //gse non c'� � 0

			//add the word or change the value
			valore += t2;
			
			this.words.put(t.getKey(), valore);
		}

		this.numOfDocs++;

	}

}

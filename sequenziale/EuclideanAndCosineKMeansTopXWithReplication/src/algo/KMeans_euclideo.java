package algo;

import java.util.Iterator;
import java.util.Map.Entry;

public class KMeans_euclideo extends KMeans {
	
	public  double calculateDistance(Document d1, Document d2) {

		double distance = 0;
		double d;

		//iterator of the map
		Iterator<Entry<String, Double>> iter = d1.getMap().entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, Double> t = iter.next();
			double valore = (double)d2.getMap().getOrDefault(t.getKey(), (double) 0);
			d = (Double)t.getValue() - valore;
			distance += d*d;
		}
		
		//the values that are not in the first document
		iter = d2.getMap().entrySet().iterator();
		while(iter.hasNext()) {

			Entry<String, Double> t = iter.next();
			if(!d1.getMap().containsKey(t.getKey())) {
				d = (Double)t.getValue();
				distance += d*d;
			}
		}

		return distance;

	}

}

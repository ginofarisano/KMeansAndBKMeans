package org.kmeans.distances;

import java.util.Iterator;
import java.util.Map.Entry;

import org.kmeans.bisec.Document;

public class EuclideanDistance implements DiscanceStrategy {

	public double distance(Document features, Document centroids) {

		double distance = 0;
		// iterator of the map
		Iterator<Entry<String, Double>> iterator = features.getMap().entrySet()
				.iterator();
		double value;
		Entry<String, Double> word;
		double dist=0;
		while (iterator.hasNext()) {
			word = iterator.next();
			value = (double) centroids.getMap().getOrDefault(word.getKey(),
					(double) 0); // getOrDefault
			dist = (Double) word.getValue() - value;
			distance += dist * dist;
		}
		// other value in the centroid
		iterator = centroids.getMap().entrySet().iterator();
		while (iterator.hasNext()) {
			word = iterator.next();
			if (!features.getMap().containsKey(word.getKey())) {
				dist = (Double) word.getValue();
				distance += dist * dist;
			}
		}
		//return Math.sqrt(distance);
		return distance;
	}
}

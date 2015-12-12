package org.kmeans.distances;

import java.util.Iterator;
import java.util.Map.Entry;

import org.kmeans.bisec.Document;

public class CosiceDistance implements DiscanceStrategy {

	public double distance(Document features, Document centroids) {
		
		double dividendo = 0, divisore = 0, aQuad = 0, bQuad = 0;
		double valore, aVal, bVal;

		/*
		 dividendo = sum(features*centroids)
		 */

		//itero su tutti i valori del primo documento
		Iterator<Entry<String, Double>> iter = features.getMap().entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, Double> t = iter.next();
			//le parole di features stanno in centroids?
			valore = (double)centroids.getMap().getOrDefault(t.getKey(), (double) 0);  //getOrDefault
			aVal = (Double)t.getValue();
			dividendo += aVal * valore;

			//prendo le a^2 per il divisore
			aQuad += aVal * aVal;
		}

		/*
		divisore = radice( sum(a^2) * sum(b^2) )
		 */

		//mi servono le b
		iter = centroids.getMap().entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, Double> t = iter.next();
			bVal = (Double)t.getValue();
			bQuad += bVal * bVal;
		}

		divisore = Math.sqrt(aQuad*bQuad);

		return 1-(dividendo/divisore);

		
	}

}

package algo;

import java.util.Iterator;
import java.util.Map.Entry;

public class KMeans_cosine extends KMeans {

	@Override
	public double calculateDistance(Document d1, Document d2) {
		double dividend = 0, divisor = 0, aQuad = 0, bQuad = 0;
		double value, aVal, bVal;

	
		Iterator<Entry<String, Double>> iter = d1.getMap().entrySet().iterator();
		
		while(iter.hasNext()) {
			Entry<String, Double> t = iter.next();

			value = (double)d2.getMap().getOrDefault(t.getKey(), (double) 0);  //getOrDefault
			aVal = (Double)t.getValue();
			dividend += aVal * value;
			aQuad += aVal * aVal;
		}

		iter = d2.getMap().entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, Double> t = iter.next();
			bVal = (Double)t.getValue();
			bQuad += bVal * bVal;
		}

		divisor = Math.sqrt(aQuad*bQuad);
	
		return 1-(dividend/divisor);
	}

}

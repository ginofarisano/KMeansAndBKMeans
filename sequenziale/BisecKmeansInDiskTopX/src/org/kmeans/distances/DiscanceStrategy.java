package org.kmeans.distances;

import org.kmeans.bisec.Document;

/**
 * select a runtime a specify algoritm
 * @author ginofarisano
 *
 */
public interface DiscanceStrategy {
	public double distance(Document features, Document centroids);
}

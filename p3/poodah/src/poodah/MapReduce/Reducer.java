package poodah.MapReduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import poodah.io.utils.OutputCollector;

/**
 * The client's reducer must implement this interface. All (K,V) pairs
 * must be serializable objects. It is imperative that (K,V) pairs
 * are consistent throughout client's project.
 * @author rajagarw
 *
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public interface Reducer<K1 extends Serializable,V1 extends Serializable,
						 K2 extends Serializable,V2 extends Serializable> {

	public void reduce(K1 key, List<V1> values, OutputCollector<K2,V2> collector)
		throws IOException;
	
}

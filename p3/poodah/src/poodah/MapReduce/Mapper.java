package poodah.MapReduce;

import java.io.IOException;
import java.io.Serializable;

import poodah.io.utils.OutputCollector;

/**
 * The client's mapper must implement this class. All K,V pairs must be
 * serializable objects. It is imperative that K,V pairs are consistent 
 * throughout all objects. Generic-type exceptions may be generated otherwise.
 * @author rajagarw, keweiz
 *
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public interface Mapper<K1 extends Serializable, V1 extends Serializable, 
						K2 extends Serializable, V2 extends Serializable> {
	
	public void map(K1 key, V1 value, OutputCollector<K2, V2> collector)
		throws IOException;
	
}

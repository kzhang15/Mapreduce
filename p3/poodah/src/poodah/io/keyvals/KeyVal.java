package poodah.io.keyvals;

import java.io.Serializable;

/**
 * This interface allows the map-reduce framework to store data into files
 * as key-value pairs. All aspects of the implementing class must be serializable.
 * The <K, V> generic types must match the generic types in the client's map
 * and reduce classes.
 * @author rajagarw, keweiz
 * @param <K> - Key type
 * @param <V> - Value type
 */
public interface KeyVal<K extends Serializable, V extends Serializable> 
	extends Serializable{

	public void setKey(K k);
	
	public K getKey();
	
	public void setValue(V v);
	
	public V getValue();
}

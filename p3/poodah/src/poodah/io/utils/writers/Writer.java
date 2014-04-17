package poodah.io.utils.writers;

import java.io.Serializable;

import poodah.io.keyvals.KeyVal;
/**
 * OutputFormat uses writer to write the final result
 * @author keweiz
 *
 * @param <K>
 * @param <V>
 */
public interface Writer<K extends Serializable, V extends Serializable> {

	public void write(KeyVal<K, V> kv);
	
	public void close();
	
}

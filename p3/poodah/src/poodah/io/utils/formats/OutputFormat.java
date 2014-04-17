package poodah.io.utils.formats;

import java.io.FileNotFoundException;
import java.io.Serializable;

import poodah.io.utils.writers.Writer;
/**
 * OutputFormat tells poodah how to output the file result
 * @author keweiz
 *
 * @param <K>
 * @param <V>
 */
public interface OutputFormat<K extends Serializable, V extends Serializable> {
	
	/**
	 * This will return a writer class that the client will implement. This
	 * write is responsible for writing the KeyVal<K,V> pairs of their choosing
	 * out to a filename.
	 * @param outputFile
	 * @return
	 * @throws FileNotFoundException
	 */
	public Writer<K, V> getWriter(String outputFile) throws FileNotFoundException;
	
}

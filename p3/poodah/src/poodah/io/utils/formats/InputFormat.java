package poodah.io.utils.formats;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import poodah.io.keyvals.KeyVal;
import poodah.io.utils.FileMeta;
/**
 * InputFormat tells poodah how to read input files
 * @author keweiz, rajagarw
 *
 * @param <K>
 * @param <V>
 */
public interface InputFormat<K extends Serializable, V extends Serializable> {
	
	/**
	 * A useful function that gets the meta data for an input file. This should be
	 * called before the work is divied to the mappers
	 * @param fileName
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public FileMeta getMetaData(String fileName) throws IOException, FileNotFoundException;
	
	/**
	 * An important class that reads data from the file and converts them into
	 * key-value pairs.
	 * @param fileName - File to read
	 * @param start - The start index of the file
	 * @param end - The end index of the file
	 * @return - A list of KeyVal (class defined by user) pairs.
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public List<KeyVal<K, V>> read(String fileName, int start, int end)  throws IOException, FileNotFoundException;
	
}

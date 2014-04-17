package poodah.io.utils;

import java.io.IOException;
import java.io.Serializable;

import poodah.conf.JobConfig;
import poodah.io.keyvals.KeyVal;
import poodah.io.keyvals.TextIntOutputKeyVal;
/**
 * OutputCollector is pass into Mappers and Reducers. 
 * It allows for the application programmer to output the results
 * @author keweiz, rajagarw
 *
 * @param <K>
 * @param <V>
 */
public abstract class OutputCollector<K extends Serializable, V extends Serializable>
implements Serializable{

	private static final long serialVersionUID = -1479155641610359521L;

	protected int numEntries = 0;
	protected String outputFile;
	protected RecordIO rio;
	protected JobConfig conf;

	public OutputCollector(String outputFile, JobConfig conf) throws IOException{
		this.outputFile = outputFile;

		rio = new RecordIO(outputFile, 0, conf);

		this.conf = conf;
		rio.openIO();
	}

	/**
	 * A method that takes in a key value from the map/reduce output and
	 * writes it too file. This method needs to be called by the client's code
	 * to make sure that the output of their functions can be processed. 
	 * @param key
	 * @param value
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public abstract void collect(K key, V value) 
		throws IOException, InstantiationException, IllegalAccessException;

	/**
	 * Closes the output stream
	 * @throws IOException
	 */
	public void closeIO() throws IOException {
		rio.closeIO();
	}

	/**
	 * Get's the number of entries that were collected
	 * @return
	 */
	public int getNumEntries(){
		return numEntries;
	}

	/**
	 * Returns a fileMeta representation of the collected keyvals. This should
	 * be called only after the map/reduce phase is done.
	 */
	public FileMeta getFileMeta(){
		RecordFileMeta fm = new RecordFileMeta();
		fm.setFileName(outputFile);
		fm.setSize(numEntries);
		return fm;
	}

	/**
	 * Sort the keyvals by key based on user comparator function
	 * @throws ClassNotFoundException
	 * @throws IllegalArgumentException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void mapSort() throws ClassNotFoundException, IllegalArgumentException,
	InstantiationException, IllegalAccessException, IOException {
		rio.mapSort();
		return;
	}

	public void reduceSort() throws ClassNotFoundException, IllegalArgumentException,
	InstantiationException, IllegalAccessException, IOException {
		rio.reduceSort();
		return;
	}

}


package poodah.conf;

import java.io.Serializable;
import java.util.Comparator;

import poodah.MapReduce.Mapper;
import poodah.MapReduce.Reducer;
import poodah.io.keyvals.KeyVal;
import poodah.io.utils.formats.InputFormat;
import poodah.io.utils.formats.OutputFormat;

/**
 * This class is crucial in the running of the client's map-reduce application.
 * It is imperative that all information pertaining to client's map/reduce classes
 * are set in this file. The map-reduce framework will make heavy use of
 * this configuration file.
 * @author rajagarw, keweiz
 *
 */
public class JobConfig implements Serializable{
	
	private static final long serialVersionUID = 1419002553179904436L;
	private Class<Mapper<?,?,?,?>> mapper = null;
	private Class<Reducer<?,?,?,?>> reducer = null;
	
	private Class<Comparator<?>> mapCompare = null;
	private Class<Comparator<?>> reduceCompare = null;
	private Class<InputFormat<?,?>> inputFormat = null;
	private Class<OutputFormat<?,?>> outputFormat = null;
	private Class<KeyVal<?,?>> mapOutputKeyVal = null;
	private Class<KeyVal<?,?>> reduceOutputKeyVal = null;

	private String inputFile = null;
	private String outputFile = null;
	
	private String clientIP = null;
	
	private int recordByteLength = 256;
	private int readerSize = 16;
	private int numKeysReducer = 4;
	private String masterServer;
	
	/**
	 * @return - Returns the Comparator class used to compare the keys together
	 */
	public Class<Comparator<?>> getReduceCompare() {
		return reduceCompare;
	}
	
	/**
	 * This method set the key comparator class. It is vital for the sorting
	 * functions and it must be supplied by the client. See MyComparator class
	 * for an example
	 * @param keyCompare - Comparator class for keys
	 */
	public void setReduceCompare(Class<? extends Comparator<?>> keyCompare) {
		this.reduceCompare = (Class<Comparator<?>>) keyCompare;
	}
	/**
	 * @return - Returns the Comparator class used to compare the keys together
	 */
	public Class<Comparator<?>> getMapCompare() {
		return mapCompare;
	}
	
	/**
	 * This method set the key comparator class. It is vital for the sorting
	 * functions and it must be supplied by the client. See MyComparator class
	 * for an example
	 * @param keyCompare - Comparator class for keys
	 */
	public void setMapCompare(Class<? extends Comparator<?>> keyCompare) {
		this.mapCompare = (Class<Comparator<?>>) keyCompare;
	}
	
	/**
	 * Returns the mapper class
	 * @return - Mapper<K1, V1, K2, V2> class
	 */
	public Class<Mapper<?,?,?,?>> getMapper() {
		return mapper;
	}
	
	/**
	 * You must pass in your mapper class in order for poodah to perform
	 * map-reduce. The Mapper requires a K1, V1, K2, V2 and it must be consistent
	 * throughout the entire project set up.
	 * Note: (K1, V1) -> (K2, V2).
	 */
	@SuppressWarnings("unchecked")
	public void setMapper(Class<? extends Mapper<?,?,?,?>> mapper) {
		this.mapper = (Class<Mapper<?, ?, ?, ?>>) mapper;
	}
	
	/**
	 * Returns the reducer class
	 * @return - Reducer<K2, V2, K3, V3> class
	 */
	public Class<Reducer<?,?,?,?>> getReducer() {
		return reducer;
	}
	/**
	 * You must pass in your Reducer class in order for poodah to perform
	 * map-reduce. The Reducer requires a K2, V2, K3, V3 and it must be consistent
	 * throughout the entire project set up.
	 * Note: (K2, V2) -> (K3, V3) where (K2, V2) was the output of the map
	 */
	@SuppressWarnings("unchecked")
	public void setReducer(Class<? extends Reducer<?,?,?,?>> reducer) {
		this.reducer = (Class<Reducer<?, ?, ?, ?>>) reducer;
	}
	
	/**
	 * @return - Returns the file that client uses as input
	 */
	public String getInputFile() {
		return inputFile;
	}
	
	/**
	 * Client must set the input file (full path) to perform the map reduce on.
	 * @param inputFile
	 */
	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}
	
	/**
	 * @return - Returns the file (full path) that client wishes to output to
	 */
	public String getOutputFile() {
		return outputFile;
	}
	/**
	 * Client must set the output file (full path) to output map-reduce results.
	 * @param inputFile
	 */
	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}
	
	/**
	 * @return - Returns client-set fixed-record length. See setRecordByteLength
	 * for more detail.
	 */
	public int getRecordByteLength() {
		return recordByteLength;
	}
	
	/**
	 * Assuming fixed-width length records, client can set this. For large data
	 * problems, this should be larger so all keys/values can fit in one record
	 */
	public void setRecordByteLength(int recordByteLength) {
		this.recordByteLength = recordByteLength;
	}
	
	/**
	 * @return - Gets the input format set by the user
	 */
	public Class<InputFormat<?,?>> getInputFormat() {
		return inputFormat;
	}
	
	/**
	 * Client will set an input format for reading in their file. TextInputFormat
	 * is an example class of this. The format must convert the input file into
	 * a (K1, V1) type for input into the Mapper class. If the types don't match up, 
	 * client will get unexpected behavior (most likely an error). Note, Client's
	 * InputFormat will generally require a KeyVal for input types. It is supposed
	 * to convert t
	 * @param inputFormat
	 */
	public void setInputFormat(Class<? extends InputFormat> inputFormat) {
		this.inputFormat = (Class<InputFormat<?, ?>>) inputFormat;
	}
	
	/**
	 * This class is responsible for writing type (K3, V3) to a file. The
	 * client must create this class so that our reducer knows how to output
	 * the data. See TextIntOuputFormat as an example.
	 * @param outputFormat
	 */
	public void setOutputFormat(Class<? extends OutputFormat> outputFormat) {
		this.outputFormat = (Class<OutputFormat<?, ?>>) outputFormat;
	}
	
	/**
	 * @return -  Returns the output format given by the client
	 */
	public Class<OutputFormat<?,?>> getOutputFormat() {
		return outputFormat;
	}
	
	/**
	 * The output key-value pair. This is required so that the output collector
	 * can create the proper (K3, V3).
	 * @param outputKeyVal
	 */
	public void setMapOutputKeyVal(Class<? extends KeyVal> outputKeyVal) {
		this.mapOutputKeyVal = (Class<KeyVal<?, ?>>)outputKeyVal;
	}

	/**
	 * @return - Returns the output key val set by the master
	 */
	public Class<KeyVal<?,?>> getMapOutputKeyVal() {
		return mapOutputKeyVal;
	}
	/**
	 * The output key-value pair. This is required so that the output collector
	 * can create the proper (K3, V3).
	 * @param outputKeyVal
	 */
	public void setReduceOutputKeyVal(Class<? extends KeyVal> outputKeyVal) {
		this.reduceOutputKeyVal = (Class<KeyVal<?, ?>>)outputKeyVal;
	}

	/**
	 * @return - Returns the output key val set by the master
	 */
	public Class<KeyVal<?,?>> getReduceOutputKeyVal() {
		return reduceOutputKeyVal;
	}
	/**
	 * @return - The reader size that the client set.
	 */
	public int getReaderSize() {
		return readerSize;
	}
	/**
	 * The size you set (bytes or lines depending on your project) that you
	 * pass into your Mapper.
	 * @return
	 */
	public void setReaderSize(int readerSize) {
		this.readerSize = readerSize;
	}
	
	
	/**
	 * @return - The number of keys the reducer should read and reduce at a time.
	 */
	public int getNumKeysReducer() {
		return numKeysReducer;
	}

	/**
	 * This is the number of keys the reducer will take in and reduce at a time
	 * @return
	 */
	public void setNumKeysReducer(int numKeysReducer) {
		this.numKeysReducer = numKeysReducer;
	}

	/**
	 * This is the ip-address of hostname of the masterServer. Needs to be
	 * set so the framework can use it.
	 * @param masterServer
	 */
	public void setMasterServer(String masterServer) {
		this.masterServer = masterServer;
	}
	
	/**
	 * This is the ip-address of hostname of the masterServer.
	 * @param masterServer
	 */
	public String getMasterServer() {
		return masterServer;
	}

	/**
	 * Client IP of who launched the job. This is autoset by JobClient.runJob()
	 * @param string
	 */
	public void setClientIp(String string) {
		// TODO Auto-generated method stub
		this.clientIP = string;
	}
	
	/**
	 * Gets client IP address that ran the job
	 * @return
	 */
	public String getClientIp(){
		return this.clientIP;
	}
}

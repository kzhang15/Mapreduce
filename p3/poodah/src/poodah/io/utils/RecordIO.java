package poodah.io.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.Comparator;

import poodah.conf.JobConfig;
import poodah.io.keyvals.KeyVal;
/**
 * RecordIO provides ways to modify a 
 * record file 
 * @author keweiz
 *
 */
public class RecordIO {
	private int numRecords;
	private JobConfig conf;
	private RandomAccessFile raf;
	private File file;
	private Comparator mapCompare;
	private Comparator reduceCompare;
	
	public RecordIO(String fileName, int numRecords, JobConfig conf) throws
	IOException{
		this.numRecords = numRecords;
		this.conf = conf;
		this.file = new File(fileName);
		try {
			this.mapCompare = conf.getMapCompare().newInstance();
			this.reduceCompare = conf.getReduceCompare().newInstance();
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		}
		
		file.createNewFile();
	}
	
	public int getNumRecords(){
		return this.numRecords;
	}
	
	public int mapCompare(KeyVal<?,?> object, KeyVal<?,?> object2) {
		
		return mapCompare.compare(object.getKey(),object2.getKey());
	}
	
	public int reduceCompare(KeyVal<?,?> object, KeyVal<?,?> object2) {
		
		return reduceCompare.compare(object.getKey(),object2.getKey());
	}
	
	public byte[] readRecord(int index) throws IOException, IllegalArgumentException, ClassNotFoundException{
	
		if(index >= numRecords){
			throw new IllegalArgumentException("Index has to be less than number of records");
		}
		int offset = index*conf.getRecordByteLength();
		byte[] b = new byte[conf.getRecordByteLength()];
		raf.seek(offset);
		int n = raf.read(b);
		if(n < conf.getRecordByteLength()){
			raf.close();
			throw new IllegalArgumentException("Record length not of correct size");
		}
		return b;
	}
	public void swapRecord(int index1, int index2) throws IllegalArgumentException, 
	ClassNotFoundException, IOException{
		
		byte[] temp1 = readRecord(index1);
		byte[] temp2 = readRecord(index2);
		writeToIndex(index1,temp2);
		writeToIndex(index2,temp1);
		return;
	}
	public void appendRecord(byte[] newRecord) throws IOException{
		if (newRecord.length>conf.getRecordByteLength())
			throw new IllegalArgumentException
			("Need to increase record byte length");

		newRecord = packArray(newRecord);
		int offset = numRecords*conf.getRecordByteLength();
		raf.write(newRecord);
		numRecords++;
		return;
	}
	private byte[] packArray(byte[] newRecord) {
		byte[] b = new byte[conf.getRecordByteLength()];
		for (int i =0; i<newRecord.length; i++) {
			b[i] = newRecord[i];
		}
		return b;
	}
	public void writeToIndex(int index, byte[] record) throws IOException{
		record = packArray(record);
		if(index >= numRecords){
			throw new IllegalArgumentException("Index has to be less than number of records");
		}
		if (record.length != conf.getRecordByteLength()) {
			throw new IllegalArgumentException("Record size is not correct");
		}
		int offset = index*conf.getRecordByteLength();
		raf.seek(offset);
		raf.write(record);
		return;
	}

	public KeyVal<?,?> deserialize(byte[] b) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(b);
		ObjectInputStream o = new ObjectInputStream(bis);
		return (KeyVal<?,?>)o.readObject();
	}
	public byte[] serialize(KeyVal<?,?> obj) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(obj);
		return b.toByteArray();
	}
	
	public void closeIO() throws IOException {
		raf.close();
	}
	public void openIO() throws IOException {
		raf = new RandomAccessFile(file,"rw");
	}
	
	public void mapSort() throws ClassNotFoundException, IllegalArgumentException, 
	IOException, InstantiationException, IllegalAccessException {
		KeyVal<?,?> kv;
		int i;
		for (int j = 1; j<numRecords; j++) {
			kv = deserialize(readRecord(j));
			i = j;
			while (i> 0 && mapCompare(deserialize(readRecord(i-1)),kv) > 0) {
				writeToIndex(i,readRecord(i-1));
				i = i - 1;
			}
			writeToIndex(i,serialize(kv));
		}
	}
	
	public void reduceSort() throws ClassNotFoundException, IllegalArgumentException, 
	IOException, InstantiationException, IllegalAccessException {
		KeyVal<?,?> kv;
		int i;
		for (int j = 1; j<numRecords; j++) {
			kv = deserialize(readRecord(j));
			i = j;
			while (i> 0 && reduceCompare(deserialize(readRecord(i-1)),kv) > 0) {
				writeToIndex(i,readRecord(i-1));
				i = i - 1;
			}
			writeToIndex(i,serialize(kv));
		}
	}
	
	
		
}
package poodah.io.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import poodah.conf.JobConfig;
import poodah.io.keyvals.KeyVal;
import poodah.io.utils.formats.OutputFormat;
import poodah.io.utils.writers.Writer;
/**
 * FileIO provides many file related operations.
 * @author keweiz, rajagarw
 *
 */
public class FileIO {

	/**
	 * 
	 * Merge all file inputs as files and output the result as 
	 * a single file to outputname
	 * Then delete all the used files
	 * @param inputFMs
	 * @param conf
	 * @param outputName
	 * @return
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws ClassNotFoundException
	 */
	public static FileMeta merge(List<FileMeta> inputFMs, 
			JobConfig conf, String outputName) throws 
			IOException, IllegalArgumentException, 
			ClassNotFoundException {
		
		RecordIO outputRO = new RecordIO(outputName
				,0,conf);
		outputRO.openIO();
		int sum = 0;
		for (int i = 0; i< inputFMs.size(); i++) {
			
			FileMeta temp = inputFMs.get(i);
			sum += temp.getSize();
			RecordIO inputRO = new RecordIO(
					temp.getFileName(), temp.getSize(), conf);
			inputRO.openIO();
			for (int j = 0; j<inputRO.getNumRecords(); j++) {
				outputRO.appendRecord(inputRO.readRecord(j));
			}
			inputRO.closeIO();
		}
		FileMeta fm = new RecordFileMeta();
		fm.setFileName(outputName);
		fm.setSize(outputRO.getNumRecords());
		outputRO.closeIO();
		deleteFiles(inputFMs);
		return fm;
	}

	/**
	 * Merge all the files, sort them 
	 * then output them to outputName
	 * delete all the used files
	 * @param inputFMs
	 * @param conf
	 * @param outputName
	 * @return
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */	
	public static FileMeta mergeSort(List<FileMeta> inputFMs, 
			JobConfig conf, String outputName, boolean isMap) throws 
			IOException, IllegalArgumentException, 
			ClassNotFoundException, InstantiationException, IllegalAccessException {
		FileMeta result;
		if (inputFMs.size() == 0) {
			result = new RecordFileMeta();
			result.setFileName(outputName);
			result.setSize(0);
			return result;
		}
		Queue<FileMeta> q = new ConcurrentLinkedQueue<FileMeta>(inputFMs);
		while (q.size() > 1) {
			FileMeta temp1 = q.poll();
			FileMeta temp2 = q.poll();
			result = mergeSortHelper(temp1, temp2, conf, isMap);
			q.add(result);
		}
		FileMeta fm = q.poll();
		File file1 = new File(fm.getFileName());
		File file2 = new File(outputName);
		file1.renameTo(file2);
		result = new RecordFileMeta();
		result.setFileName(outputName);
		result.setSize(fm.getSize());
		return result;
	}
		
	private static FileMeta mergeSortHelper(FileMeta fm1, FileMeta fm2, JobConfig conf, boolean isMap) 
	throws IOException, ClassNotFoundException, IllegalArgumentException, 
	InstantiationException, IllegalAccessException {

		FileMeta result = new RecordFileMeta();
		result.setFileName(fm1.getFileName() + ".temp");
		result.setSize(0);
		RecordIO rio1 = new RecordIO(fm1.getFileName(),fm1.getSize(), conf);
		RecordIO rio2 = new RecordIO(fm2.getFileName(), fm2.getSize(), conf);
		RecordIO rio3 = new RecordIO(result.getFileName(), 0, conf);
		rio1.openIO();
		rio2.openIO();
		rio3.openIO();
		int i = 0;
		int j = 0;
		while (i< fm1.getSize() && j < fm2.getSize()) {
			KeyVal kv1 = rio3.deserialize(rio1.readRecord(i));
			KeyVal kv2 = rio3.deserialize(rio2.readRecord(j));
			if(isMap){
			if (rio3.mapCompare(kv1, kv2) < 0) {
				rio3.appendRecord(rio3.serialize(kv1));
				i++;
			} else {
				rio3.appendRecord(rio3.serialize(kv2));
				j++;
			}
			} else{
				if (rio3.reduceCompare(kv1, kv2) < 0) {
					rio3.appendRecord(rio3.serialize(kv1));
					i++;
				} else {
					rio3.appendRecord(rio3.serialize(kv2));
					j++;
				}
			}
		}
		if (i< fm1.getSize()) {
			for (;i < fm1.getSize(); i++) {
				KeyVal kv = rio3.deserialize(rio1.readRecord(i));
				rio3.appendRecord(rio3.serialize(kv));
			}
		}

		if (j< fm2.getSize()) {
			for (;j < fm2.getSize(); j++) {
				KeyVal kv = rio3.deserialize(rio2.readRecord(j));
				rio3.appendRecord(rio3.serialize(kv));
			}
		}
		result.setSize(rio3.getNumRecords());
		rio1.closeIO();
		rio2.closeIO();
		rio3.closeIO();
		deleteFile(fm1);
		deleteFile(fm2);
		return result;
	}	
	
	
	/**
	 * delete a list of files
	 * @param inputFms
	 */
	private static void deleteFiles(List<FileMeta> inputFms) {
		for(FileMeta fm: inputFms){
			File temp = new File(fm.getFileName());
			temp.delete();
		}
	}
	/**
	 * delete a single file
	 * @param inputFm
	 */
	private static void deleteFile(FileMeta inputFm) {
		File temp = new File(inputFm.getFileName());
		temp.delete();
	}
	
	/**
	 * take the final file record and translate it 
	 * according to the job configuration  to 
	 * readable content 
	 * @param fm
	 * @param conf
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws ClassNotFoundException
	 */
	
	public static FileMeta translate(FileMeta fm, JobConfig conf) 
		throws InstantiationException, IllegalAccessException, IOException, 
		IllegalArgumentException, ClassNotFoundException{
		OutputFormat of = conf.getOutputFormat().newInstance();
		RecordIO rio = new RecordIO(fm.getFileName(), fm.getSize(), conf);
		rio.openIO();
		Writer w = of.getWriter(conf.getOutputFile());
		for(int i = 0; i < rio.getNumRecords(); i++){
			w.write(rio.deserialize(rio.readRecord(i)));
		}
		w.close();
		TextFileMeta tfm = new TextFileMeta();
		tfm.setFileName(conf.getOutputFile());
		tfm.setSize(rio.getNumRecords());
		rio.closeIO();
		File temp = new File(fm.getFileName());
		temp.delete();
		return tfm;
	}
	
	/**
	 * Parse the input file by keys 
	 * @param fm
	 * @param jc
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	
	public static List<RecordFileMeta> parseFile(FileMeta fm, JobConfig jc) throws IllegalArgumentException, 
	InstantiationException, IllegalAccessException, IOException, ClassNotFoundException {
		RecordIO rio = new RecordIO(fm.getFileName(), fm.getSize(), jc);
		rio.openIO();
		List<RecordFileMeta> rfmList = new ArrayList<RecordFileMeta>();
		int i = 0;
		int j = i + 1;
		int count = 0;
		int start = 0;
		while(j < rio.getNumRecords()) {
			if (rio.mapCompare(rio.deserialize(rio.readRecord(i)), 
					rio.deserialize(rio.readRecord(j))) != 0) {
				count ++;
			}
			if (count == jc.getNumKeysReducer()) {
				RecordFileMeta rfm = new RecordFileMeta();
				rfm.setStartPos(start);
				rfm.setEndPos(j);
				rfm.setFileName(fm.getFileName());
				rfm.setSize(fm.getSize());
				start = j;
				rfmList.add(rfm);
				count = 0;
			}
			i++; j++;	
		}
		if (start != j) {
			RecordFileMeta rfm = new RecordFileMeta();
			rfm.setStartPos(start);
			rfm.setEndPos(j);
			rfm.setFileName(fm.getFileName());
			rfm.setSize(fm.getSize());
			start = j;
			rfmList.add(rfm);
			count = 0;
			
		}
		rio.closeIO();
		return rfmList;
	}
}

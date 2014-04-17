package poodah.io.utils.formats;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import poodah.io.keyvals.KeyVal;
import poodah.io.keyvals.TextInputKeyVal;
import poodah.io.utils.TextFileMeta;

public class TextInputFormat implements InputFormat<Integer, String>{
	
	/**
	 * Used by Master
	 */
	@Override
	public TextFileMeta getMetaData(String fileName) 
			throws IOException, FileNotFoundException {
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		int lines = 0;
		while (reader.readLine() != null) lines++;
		reader.close();
		TextFileMeta tfm = new TextFileMeta();
		tfm.setFileName(fileName);
		tfm.setSize(lines);
		return tfm;
		
	}

	/**
	 * Used by Mapper
	 */
	@Override
	public List<KeyVal<Integer, String>> 
			read(String fileName, int startLine, int endLine) 
			throws IOException, FileNotFoundException{
		List<KeyVal<Integer, String>> keyVals = 
				new ArrayList<KeyVal<Integer, String>>();
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		int lines = 0;
		while (lines < startLine) {
			reader.readLine();
			lines++;
		}
		while(lines < endLine){
			String line = reader.readLine();
			KeyVal<Integer, String> kv = new TextInputKeyVal();
			kv.setKey(lines);
			kv.setValue(line);
			keyVals.add(kv);
			lines++;
		}
		reader.close();
		return keyVals;
		
	}

}

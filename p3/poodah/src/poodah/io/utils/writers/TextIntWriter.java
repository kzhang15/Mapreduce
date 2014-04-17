package poodah.io.utils.writers;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import poodah.io.keyvals.KeyVal;

public class TextIntWriter implements Writer<String, Integer>{
	
	FileOutputStream out;
	PrintStream p;
	
	public TextIntWriter(String outputFile) throws FileNotFoundException{
		out = new FileOutputStream(outputFile);
		p = new PrintStream(out);
	}
	@Override
	public void write(KeyVal<String, Integer> kv) {
		p.println(kv.getKey() + "," + kv.getValue());		
	}
	@Override
	public void close() {
		p.close();			
	}
	
}

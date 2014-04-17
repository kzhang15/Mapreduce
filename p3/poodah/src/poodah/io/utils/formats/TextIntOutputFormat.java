package poodah.io.utils.formats;

import java.io.FileNotFoundException;

import poodah.io.utils.writers.TextIntWriter;
import poodah.io.utils.writers.Writer;

public class TextIntOutputFormat implements OutputFormat<String, Integer>{
	
	@Override
	public Writer<String, Integer> getWriter(String outputFile) 
		throws FileNotFoundException{
		return new TextIntWriter(outputFile);
	}

}

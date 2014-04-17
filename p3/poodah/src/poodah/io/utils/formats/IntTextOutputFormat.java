package poodah.io.utils.formats;

import java.io.FileNotFoundException;

import poodah.io.utils.writers.IntTextWriter;
import poodah.io.utils.writers.Writer;


public class IntTextOutputFormat implements OutputFormat<Integer, String>{

	@Override
	public Writer<Integer, String> getWriter(String outputFile) 
	throws FileNotFoundException{
		return new IntTextWriter(outputFile);
	}

}


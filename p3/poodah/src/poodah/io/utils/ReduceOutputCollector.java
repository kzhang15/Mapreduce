package poodah.io.utils;

import java.io.IOException;
import java.io.Serializable;

import poodah.conf.JobConfig;
import poodah.io.keyvals.KeyVal;

public class ReduceOutputCollector <K extends Serializable, V extends Serializable> 
	extends OutputCollector<K, V>{

	public ReduceOutputCollector(String outputFile, JobConfig conf)
			throws IOException {
		super(outputFile, conf);
	}

	@Override
	public void collect(K key, V value) throws IOException,
			InstantiationException, IllegalAccessException {
		KeyVal kv = conf.getReduceOutputKeyVal().newInstance();
		kv.setKey(key);
		kv.setValue(value);
		rio.appendRecord(rio.serialize(kv));
		numEntries++;		
	}

}

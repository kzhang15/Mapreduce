package poodah.io.utils;

import java.io.IOException;
import java.io.Serializable;

import poodah.conf.JobConfig;
import poodah.io.keyvals.KeyVal;

public class MapOutputCollector<K extends Serializable, V extends Serializable> 
	extends OutputCollector<K, V>{

	private static final long serialVersionUID = -7287945377348123006L;

	public MapOutputCollector(String outputFile, JobConfig conf)
	throws IOException {
		super(outputFile, conf);
	}

	public void collect(K key, V value) 
		throws IOException, InstantiationException, IllegalAccessException{
		KeyVal kv = conf.getMapOutputKeyVal().newInstance();
		kv.setKey(key);
		kv.setValue(value);
		rio.appendRecord(rio.serialize(kv));
		numEntries++;
	}

}

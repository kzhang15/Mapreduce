package poodah.startup;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;

import poodah.conf.JobConfig;
import poodah.processes.messages.incoming.JobMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.ExtraUtils;

/**
 * Job client sends the job configuration to the master. 
 * It also makes sure that configuration is proper.
 * @author keweiz
 *
 */

public class JobClient {

	public static int runJob(JobConfig conf){
		String masterServer = conf.getMasterServer();
		JobMessage request = new JobMessage();
		request.setConf(conf);
		try {	
			if (conf.getReducer() == null){
				System.err.println("Please set a reducer");
				return -1;
			}
			if (conf.getInputFile() == null ) {
				System.err.println("Please set an input file");
				return -1;
			}
			if (conf.getInputFormat()== null) {
				System.err.println("Please set an input format"); 
				return -1;
			}
			if (conf.getMapCompare() == null) {
				System.err.println("Please set a map-key compare function");
				return -1;
			}
			if (conf.getReduceCompare() == null) {
				System.err.println("Please set a reduce-key compare function");
				return -1;
			}
			if (conf.getMapper() == null) {
				System.err.println("Please set mapper class");
				return -1;
			}
			if (conf.getMasterServer() == null) {
				System.err.println("Please set master server");
				return -1;
			}
			if (conf.getNumKeysReducer() < 1) {	
				System.err.println("Please number of reducer keys");
				return -1;
			}
			if (conf.getOutputFile() == null) {
				System.err.println("Please set output file");
				return -1;
			}
			if (conf.getOutputFormat() == null) {
				System.err.println("Please set the output format");
				return -1;
			}
			if (conf.getReduceOutputKeyVal() == null) {
				System.err.println("Please set the reduce output key val");
				return -1;
			}
			if (conf.getMapOutputKeyVal() == null) {
				System.err.println("Please set the map output key val");
				return -1;
			}
			if (conf.getReaderSize() < 1)  {
				System.err.println("Please set the reader size");
				return -1;
			}
			if (conf.getRecordByteLength() < 1) {
				System.err.println("Please set the fixed record byte length");
				return -1;
			}
			conf.setClientIp(InetAddress.getLocalHost().getHostAddress().toString());
			JobMessage response = (JobMessage) ExtraUtils.sendObject(request, masterServer, 
					Constants.NEW_JOB_PORT);
			return response.getJobId();
			
		} catch (ConnectException e) {
			return -1;
		} catch (IOException e) {
			return -1;
		}
	}

}

package poodah.processes.listeners.worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import poodah.MapReduce.Mapper;
import poodah.MapReduce.Reducer;
import poodah.conf.JobConfig;
import poodah.io.keyvals.KeyVal;
import poodah.io.utils.FileMeta;
import poodah.io.utils.MapOutputCollector;
import poodah.io.utils.OutputCollector;
import poodah.io.utils.RecordFileMeta;
import poodah.io.utils.RecordIO;
import poodah.io.utils.ReduceOutputCollector;
import poodah.io.utils.formats.InputFormat;
import poodah.processes.listeners.Listener;
import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.AppendMessage;
import poodah.processes.messages.incoming.ErrorMessage;
import poodah.processes.messages.incoming.ShuffleMessage;
import poodah.processes.messages.outgoing.MapMessage;
import poodah.processes.messages.outgoing.ReduceMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.ExtraUtils;
import poodah.processes.utils.Strategy;

/**
 * This listener is spawned from the participants. It takes care of both
 * the reduction and mapping depending on what the master gives the participant. 
 * @author rajagarw, keweiz
 */
public class WorkerListener extends Listener {

	static final int PORT = Constants.WORKER_PORT;
	private String masterIP;
	private String myIP;
	
	public WorkerListener(String masterIP, String myIP){
		super(PORT);
		this.myIP = myIP;
		this.masterIP = masterIP;
		strat = new WorkerStrategy();
	}
	
	private class ReduceCompute implements Runnable {
		private ReduceMessage in;
		
		public ReduceCompute(ReduceMessage in){
			this.in = in;
		}
		
		/**
		 * Alot of assumptions are made here about the generic types of 
		 * the user's input classes. We assume all the generic types will 
		 * match up.
		 */
		@Override
		public void run() {
			JobConfig conf = in.getConf();
			try{
				OutputCollector oc = new ReduceOutputCollector(in.getOutputFile(), conf);
				Reducer clientReducer = conf.getReducer().newInstance();
				RecordIO rio = new RecordIO(in.getFile().getFileName(), 
							in.getFile().getSize(),conf);
				rio.openIO();
				RecordFileMeta rfm = (RecordFileMeta)in.getFile();
				int i = rfm.getStartPos();
				KeyVal kv = rio.deserialize(rio.readRecord(i));
				if(rfm.getEndPos() - rfm.getStartPos() < 2){
					List vals = new ArrayList();
					vals.add(kv.getValue());
					clientReducer.reduce(kv.getKey(), vals, oc);
					FileMeta rfmOut = oc.getFileMeta();
					AppendMessage response = new AppendMessage();
					response.setFile(rfmOut);
					response.setMyIP(myIP);
					response.setJobId(in.getJobId());
					rio.closeIO();
					oc.closeIO();
					ExtraUtils.sendObject(response, masterIP, 
							Constants.APPEND_PORT);
					return;
				}
				int j = i + 1;
				Map hMap = new HashMap();
				List vals = new ArrayList();
				vals.add(kv.getValue());
				hMap.put(kv.getKey(), vals);
				KeyVal kv2 = rio.deserialize(rio.readRecord(j));
				while(j < rfm.getEndPos()){
					kv = rio.deserialize(rio.readRecord(i));
					kv2 = rio.deserialize(rio.readRecord(j));
					if(rio.reduceCompare(kv, kv2) != 0){ // The records have changed keys
						/*
						 * Assuming that file is sorted, we won't see same key after this point
						 * We will now reduce the last set of keys that just ended
						 */
						clientReducer.reduce(kv.getKey(), (List)hMap.get(kv.getKey()), oc);
						hMap.remove(kv.getKey()); // Remove this key from map
						vals = new ArrayList();
					} else{
						vals = (List)hMap.get(kv2.getKey());
					}
					vals.add(kv2.getValue());
					hMap.put(kv2.getKey(), vals);
					i++; j++;
				}
				vals = (List)hMap.get(kv2.getKey());
				clientReducer.reduce(kv2.getKey(), vals, oc);
				hMap.remove(kv.getKey());
				FileMeta rfmOut = oc.getFileMeta();
				AppendMessage response = new AppendMessage();
				response.setFile(rfmOut);
				response.setMyIP(myIP);
				response.setJobId(in.getJobId());
				oc.reduceSort();
				rio.closeIO();
				oc.closeIO();
				ExtraUtils.sendObject(response, masterIP, 
						Constants.APPEND_PORT);
			} catch(Exception e){
				try {
					new RecordIO(in.getOutputFile(), 0, conf);
					AppendMessage response = new AppendMessage();
					RecordFileMeta rfm = new RecordFileMeta();
					rfm.setFileName(in.getOutputFile());
					rfm.setSize(0);
					response.setFile(rfm);
					response.setMyIP(myIP);
					response.setJobId(in.getJobId());
					ExtraUtils.sendObject(response, masterIP, Constants.APPEND_PORT);
				} catch (IOException e1) {
					sendError(e1.getMessage(), in.getJobId());
				}
				sendError(e.getMessage(), in.getJobId());
			}
			
			/*catch(FileNotFoundException e){
				sendError(e.getMessage(), in.getJobId());
			} catch (InstantiationException e) {
				sendError(e.getMessage(), in.getJobId());
			} catch (IllegalAccessException e) {
				sendError(e.getMessage(), in.getJobId());
			} catch (IllegalArgumentException e) {
				sendError(e.getMessage(), in.getJobId());
			} catch (IOException e) {
				sendError(e.getMessage(), in.getJobId());
			} catch (ClassNotFoundException e) {
				sendError(e.getMessage(), in.getJobId());
			}*/
		}
		
		private void sendError(String message, int jobId){
			ErrorMessage em = new ErrorMessage();
			em.setJobId(jobId);
			em.setErrorMessage(message);
			try {
				ExtraUtils.sendObject(em, masterIP, Constants.COMM_MASTER_PORT);
			} catch (ConnectException e1) {
			} catch (IOException e1) {
			}
		}
	}

	private class MapCompute implements Runnable {

		private MapMessage in;

		public MapCompute(MapMessage in){
			this.in = in;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			JobConfig conf = in.getConf();
			try {
				OutputCollector oc = new MapOutputCollector(in.getOutputFile(),conf);
				Mapper clientMap = conf.getMapper().newInstance();
				//GET INPUT READER FROM JOB CONF, PASS IT INTO THIS OBJECT!
				InputFormat inputReader = conf.getInputFormat().newInstance();
				List<KeyVal<?,?>> readIn = 
						inputReader.read(conf.getInputFile(), 
								in.getStart(), in.getEnd());
				for(KeyVal kv : readIn){
					clientMap.map(kv.getKey(), kv.getValue(), oc);
				}
				oc.mapSort();
				oc.closeIO();
				FileMeta fm = oc.getFileMeta();
				ShuffleMessage response = new ShuffleMessage();
				response.setJobId(in.getJobId());
				response.setFile(fm);
				response.setMyIP(myIP);
				ExtraUtils.sendObject(response, masterIP, 
						Constants.SHUFFLE_PORT);
			} catch (Exception e){
				sendError(e.getMessage(), in.getJobId());
			}
			
			/*catch (FileNotFoundException e) {
				sendError(e.getMessage(), in.getJobId());
			} catch (IOException e) {
				sendError(e.getMessage(), in.getJobId());
			} catch (InstantiationException e) {
				sendError(e.getMessage(), in.getJobId());
			} catch (IllegalAccessException e) {
				sendError(e.getMessage(), in.getJobId());
			} catch (IllegalArgumentException e) {
				sendError(e.getMessage(), in.getJobId());
			}  catch (ClassNotFoundException e) {
				sendError(e.getMessage(), in.getJobId());
			}*/
		}
		private void sendError(String message, int jobId){
			ErrorMessage em = new ErrorMessage();
			em.setJobId(jobId);
			em.setErrorMessage(message);
			try {
				ExtraUtils.sendObject(em, masterIP, Constants.COMM_MASTER_PORT);
			} catch (ConnectException e1) {
			} catch (IOException e1) {
			}
		}
	}

	private class WorkerStrategy implements Strategy {
		@Override
		public Message compute(Message in) throws IllegalArgumentException {
			if (in == null){
				throw new IllegalArgumentException("MapMessage is null!");
			}
			if(in.getMessageType().equals(Constants.MapMessageType)){
				MapMessage input = (MapMessage)in;
				Thread t = new Thread(new MapCompute(input));
				t.start();
				return null;
			} else if(in.getMessageType().equals(Constants.ReduceMessageType)){
				ReduceMessage input = (ReduceMessage)in;
				Thread t = new Thread(new ReduceCompute(input));
				t.start();
				return null;
			} else if(in.getMessageType().equals(Constants.HeartBeatMessageType)){
				return in;
			}
			return null;
		}

	}
}

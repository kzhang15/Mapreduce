package poodah.processes.utils;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import poodah.conf.JobConfig;
import poodah.io.utils.FileIO;
import poodah.io.utils.FileMeta;
import poodah.io.utils.RecordFileMeta;
import poodah.io.utils.formats.InputFormat;
import poodah.processes.messages.incoming.ErrorMessage;
import poodah.processes.messages.outgoing.MapMessage;
import poodah.processes.messages.outgoing.ProcManMessage;
import poodah.processes.messages.outgoing.ReduceMessage;
import poodah.processes.messages.outgoing.WorkMessage;

/**
 * THIS IS THE BRAIN OF OUR FRAMEWORK. All the redistribution and data
 * that is associated with each job, each file, and each worker is registered
 * here.
 * 
 * @author rajagarw, keweiz
 */
public class DistributedMap {

	private int numWorkers;
	private int numMapsHost;
	private int numRedsHost;
	private int jobCounter = 0;
	private String mapDir;
	private String reduceDir;
	private String shuffleDir;

	private Map<String, JobHandler> mapJobs; // Initially affected by adding a worker
	private Map<String, JobHandler> reduceJobs;
	// Only affected when maps are finished (shuffle Listener) or when jobs are being added:
	private Map<Integer, Queue<String>> mapOutputJobs; // When len(val) = 0, a shuffle needs to be performed
	private Map<Integer, Queue<FileMeta>> shuffleInputJobs;
	private Map<Integer, Queue<String>> reduceOutputJobs; // When len(val) = 0, an append needs to be performed
	private Map<Integer, Queue<FileMeta>> appendInputJobs;
	private Map<Integer, JobConfig> jobToConf;
	private Queue<WorkMessage> mapWorkQueue; // Work that needs to be distributed
	private Queue<WorkMessage> reduceWorkQueue;
	private Map<Integer, FileMeta> finalOutputs;
	private Queue<String> backupWorkers;
	private Map<Integer, String> jobToProcMan;
	private Map<Integer, Integer> jobRemovalFlags;
	private String masterIP;
	private String workingDir;
	
	private Object lock;
	
	public DistributedMap(int numWorkers, int numMapsHost, int numRedsHost, 
			String mapDir, String shuffleDir, String reduceDir, String masterIP,
			String workingDir){
		
		this.masterIP = masterIP;
		this.workingDir = workingDir;
		this.mapDir = mapDir;
		this.shuffleDir = shuffleDir;
		this.reduceDir = reduceDir;
		this.numWorkers = numWorkers;
		this.numMapsHost = numMapsHost;
		this.numRedsHost = numRedsHost;
		mapJobs = new ConcurrentHashMap<String, JobHandler>();
		reduceJobs = new ConcurrentHashMap<String, JobHandler>();
		mapOutputJobs = new ConcurrentHashMap<Integer, Queue<String>>(); // Can only be strings cause these files are empty
		shuffleInputJobs = new ConcurrentHashMap<Integer, Queue<FileMeta>>();
		reduceOutputJobs = new ConcurrentHashMap<Integer, Queue<String>>(); // Can only be strings cause these files are empty
		appendInputJobs = new ConcurrentHashMap<Integer, Queue<FileMeta>>();
		jobToConf = new ConcurrentHashMap<Integer, JobConfig>();
		mapWorkQueue = new ConcurrentLinkedQueue<WorkMessage>();
		reduceWorkQueue = new ConcurrentLinkedQueue<WorkMessage>();
		finalOutputs = new ConcurrentHashMap<Integer, FileMeta>();
		backupWorkers = new ConcurrentLinkedQueue<String>();
		jobToProcMan = new ConcurrentHashMap<Integer, String>();
		jobRemovalFlags = new ConcurrentHashMap<Integer, Integer>();
		lock = new Object();
	}

	public int numParticipants(){
		return mapJobs.keySet().size() + reduceJobs.keySet().size();
	}

	/**
	 * Adds a job to the job queue, so at the next distribution, it will be
	 * round-robin distributed.
	 * @param conf - JobConfig file associated with new Job.
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public int addJob(JobConfig conf) 
			throws InstantiationException, IllegalAccessException, IOException {
		int jobId;
		synchronized(lock){
			jobId = jobCounter;
			jobCounter++;
		}

		jobToConf.put(jobId, conf); // Keep track of config files
		jobToProcMan.put(jobId, conf.getClientIp());
		jobRemovalFlags.put(jobId, 0);
		String inputFile = conf.getInputFile();
		int splitNum = conf.getReaderSize();
		InputFormat iF = conf.getInputFormat().newInstance();
		FileMeta fileData = iF.getMetaData(inputFile);
		int outNum = 0;
		for(int i = 0; i < fileData.getSize(); i+=splitNum){
			int  j = i + ((fileData.getSize() - i >= splitNum) 
					? splitNum : fileData.getSize() - i);
			String outName = mapDir + "/Map." + jobId + "." + outNum;
			if(mapOutputJobs.containsKey(jobId)){
				Queue<String> jobList = mapOutputJobs.get(jobId);
				jobList.add(outName);
				mapOutputJobs.put(jobId, jobList);
			} else{
				Queue<String> jobList = new ConcurrentLinkedQueue<String>();
				jobList.add(outName);
				mapOutputJobs.put(jobId, jobList);
			}
			MapMessage mm = new MapMessage();
			mm.setConf(conf);
			mm.setFile(fileData);
			mm.setJobId(jobId);
			mm.setOutputFile(outName);
			mm.setStart(i);
			mm.setEnd(j);
			mapWorkQueue.add(mm);
			outNum++;
		}
		distributedWork(mapJobs, mapWorkQueue, numMapsHost);
		return jobId;
	}

	public String getMasterIp(){
		return masterIP;
	}
	
	/**
	 * Looks in the workQueues to retrieve jobs, and then checks to see
	 * if any worker in the respective work map/reduceJobs is available.
	 * @throws ConnectException
	 * @throws IOException
	 */
	private synchronized void distributedWork(Map<String, JobHandler> jobMap, 
			Queue<WorkMessage> workQueue, int numJobsHost){
		Set<String> addrs = jobMap.keySet();
		if(addrs.size() == 0){
			return;
		}
		String[] addrArray = new String[addrs.size()];
		addrs.toArray(addrArray);
		for(int i = 0; i < addrArray.length; i++){
			if(workQueue.size() == 0){
				break;
			}
			JobHandler temp = jobMap.get(addrArray[i]);
			if((temp!=null) && (temp.getNumJobs() < numJobsHost)){ // Space to add a job
				WorkMessage out = workQueue.poll(); // retrieve the work order 
				if(jobRemovalFlags.get(out.getJobId())==1){
					i--; 
					continue; // If the job was flagged for removal, stop distributing it's work
				}
				temp.addJob(out.getJobId(), out); // Add it to the JobHandler
				jobMap.put(addrArray[i], temp); // Put it back in the jobMap
				try {
					ExtraUtils.sendObject(out, addrArray[i], Constants.WORKER_PORT);
					if(temp.getNumJobs() < numMapsHost) i--; // Distribute to same mapper if more threads exist
				} catch (Exception e){
					JobHandler jh = jobMap.get(addrArray[i]);
					jh.removeJob(out.getJobId(), out.getOutputFile());
					jobMap.put(addrArray[i], jh);
					ArrayList<WorkMessage> l = new ArrayList<WorkMessage>(workQueue);
					l.add(0, out);
					workQueue = new ConcurrentLinkedQueue<WorkMessage>(l);
				}
			}
		}

	}

	private String getWorkersByJobId(Map<String, JobHandler> jobMap, int jobId){
		String response = "";
		Set<String> workers = jobMap.keySet();
		for(String s : workers){
			JobHandler jh = jobMap.get(s);
			if(jh.containsJob(jobId)){
				response += s + ", ";
			}
		}
		return response;
	}

	/**
	 * Parses messages sent by the client. We also store the different
	 * process managers connected to us.
	 * @param input
	 * @param query
	 * @return
	 */
	public synchronized ProcManMessage procManResponse(ProcManMessage input, String[] query){
		String response = "";
		if(query[0].equalsIgnoreCase("monitor")){
			int jobId = -1;
			try{
				jobId = Integer.parseInt(query[1]);
			} catch (NumberFormatException e){
			}
			if(finalOutputs.containsKey(jobId)){
				response += "Job: " + jobId+ " is finished";
			} else if(!jobToConf.containsKey(jobId)){
				response += "Job doesn't exist\n";
			} else{
				response += "Mappers: ";
				response += getWorkersByJobId(mapJobs, jobId) + "\n\n";
				response += "Reducers:";
				response += getWorkersByJobId(reduceJobs, jobId) + "\n\n";
				if(mapOutputJobs.containsKey(jobId)){
					response += "Number of Maps left: " + mapOutputJobs.get(jobId).size() + "\n";	
				} else {
					response += "Number of Maps left: 0\n";
				}
				if(reduceOutputJobs.containsKey(jobId)){
					response += "Number of Reduces left: " + reduceOutputJobs.get(jobId).size() + "\n";	
				}else {
					response += "Number of Reduces left: 0\n";
				}
			}
		} else if (query[0].equalsIgnoreCase("killJob")){
			int jobId = Integer.parseInt(query[1]);
			jobRemovalFlags.put(jobId, 1);
			response += "Job: " + jobId + " flagged for removal";
		} else if(query[0].equalsIgnoreCase("stop")){
			Set<String> workers = getWorkers();
			Set<String> procMans = getAllProcMans();
			while(!backupWorkers.isEmpty())
				backupWorkers.poll();
			for(String s : workers){
				if(!s.equals(masterIP) && !procMans.contains(s)){
					ProcessBuilder pb = new ProcessBuilder();
					pb.command(workingDir + "/killworker.py", query[1], s);
					try {
						pb.start();
					} catch (IOException e) {
					}
				}
			}

			for(String s : procMans){
				ErrorMessage em = new ErrorMessage();
				em.setErrorMessage("Master is about to shutdown. Please quit.");
				try {
					ExtraUtils.sendObject(em, s, Constants.PROC_MAN_PORT);
				} catch (ConnectException e) {
				} catch (IOException e) {
				}
			}			
			ProcessBuilder pb = new ProcessBuilder();
			pb.command(workingDir + "/killworker.py", query[1], masterIP);
			try {
				pb.start();
			} catch (IOException e) {
			}
		}

		ProcManMessage pmm = new ProcManMessage();
		pmm.setResponse(response);
		return pmm;
	}

	public String getProcManIp(int jobId){
		return jobToProcMan.get(jobId);
	}

	public Set<String> getAllProcMans() {
		return new HashSet<String>(jobToProcMan.values());
	}

	public String getBackupWorker(){
		if(backupWorkers.size()==0){
			return null;
		}
		return backupWorkers.remove();
	}
	
	/**
	 * Adds a worker. If the worker already exists, we simply don't add it
	 * If we have enough workers, we add it as a backup. 
	 * @param ipAddr
	 * @return
	 */
	public synchronized boolean addWorker(String ipAddr){
		if(mapJobs.containsKey(ipAddr) || reduceJobs.containsKey(ipAddr)){
			return false;
		}
		if(mapJobs.keySet().size() + reduceJobs.keySet().size() >= numWorkers){
			backupWorkers.add(ipAddr);
			return false;
		}
		if(mapJobs.keySet().size() > reduceJobs.keySet().size()){
			reduceJobs.put(ipAddr, new JobHandler());
			distributedWork(reduceJobs, reduceWorkQueue, numRedsHost);
			return true;
		} else{
			mapJobs.put(ipAddr, new JobHandler());
			distributedWork(mapJobs, mapWorkQueue, numMapsHost);
			return true;
		}
	}

	/**
	 * This function is necessary to prep the files for reduction stage. It
	 * will then distribute the workload to each reducer
	 * @param outputFile - Output from the mapper. It's what the mapper sent as ShuffleMessage
	 * @param workerIP - Ip Address from where is came
	 * @param jobId - id of Job
	 * @throws IOException 
	 * @throws ConnectException 
	 */
	public synchronized void addShuffleJob(FileMeta outputFile, String workerIP, int jobId) throws ConnectException, IOException {
		mapJobs.get(workerIP).removeJob(jobId, outputFile.getFileName()); // Remove file from mapper -> Mapper isn't doing this file
		Queue<String> currJobs = mapOutputJobs.get(jobId); // File isn't being mapped anymore 
		currJobs.remove(outputFile.getFileName());
		if(shuffleInputJobs.containsKey(jobId)){ // Add onto shuffle input
			Queue<FileMeta> fileInputs = shuffleInputJobs.get(jobId);
			fileInputs.add(outputFile);
			shuffleInputJobs.put(jobId, fileInputs);
		} else{
			Queue<FileMeta> fileInputs = new ConcurrentLinkedQueue<FileMeta>();
			fileInputs.add(outputFile);
			shuffleInputJobs.put(jobId, fileInputs);
		}
		distributedWork(mapJobs, mapWorkQueue, numMapsHost);
		if(currJobs.size() == 0){ // If no more files left for a job, it's time to shuffle
			mapOutputJobs.remove(jobId);
			for (String s : mapJobs.keySet()){
				JobHandler jh = mapJobs.get(s);
				if(jh!=null){
					jh.removeJobId(jobId);
					mapJobs.put(s, jh);
				}
			}
			if(jobRemovalFlags.get(jobId)==1){ // If file is flagged for removal, don't let it advance
				shuffleInputJobs.remove(jobId);
				return;
			}
			String outFile = "/Shuffle." + jobId;
			List<FileMeta> shuffleFiles = new ArrayList<FileMeta>();
			for(FileMeta fm : shuffleInputJobs.get(jobId)){
				shuffleFiles.add(fm);
			}
			try {
				FileMeta mergedFile = FileIO.mergeSort(shuffleFiles, jobToConf.get(jobId), 
						shuffleDir + outFile, true);
				List<RecordFileMeta> reducedJobs = FileIO.parseFile(mergedFile, jobToConf.get(jobId));
				int outNum = 0;
				for(RecordFileMeta t : reducedJobs){
					ReduceMessage rm = new ReduceMessage();
					rm.setConf(jobToConf.get(jobId));
					String outName = reduceDir + "/" + "Reduce." + jobId + "." + outNum;
					rm.setFile(t);
					rm.setJobId(jobId);
					rm.setOutputFile(outName);
					reduceWorkQueue.add(rm);
					if(reduceOutputJobs.containsKey(jobId)){
						Queue<String> jobList = reduceOutputJobs.get(jobId);
						jobList.add(outName);
						reduceOutputJobs.put(jobId, jobList);
					} else{
						Queue<String> jobList = new ConcurrentLinkedQueue<String>();
						jobList.add(outName);
						reduceOutputJobs.put(jobId, jobList);
					}
					outNum++;
				}
				distributedWork(reduceJobs, reduceWorkQueue, numRedsHost);
				shuffleInputJobs.remove(jobId);

			} catch (Exception  e){
				ExtraUtils.sendError(masterIP, e.getMessage(), jobId);
			}
		}
	}


	public synchronized void appendFileMeta(int jobId, String workerIP, FileMeta file) 
			throws ConnectException, IOException {
		reduceJobs.get(workerIP).removeJob(jobId, file.getFileName());
		Queue<String> currJobs = reduceOutputJobs.get(jobId);
		currJobs.remove(file.getFileName());
		if(!appendInputJobs.containsKey(jobId)){
			Queue<FileMeta> fileInput = new ConcurrentLinkedQueue<FileMeta>();
			fileInput.add(file);
			appendInputJobs.put(jobId, fileInput);
		} else{
			Queue<FileMeta> fileInput = appendInputJobs.get(jobId);
			fileInput.add(file);
			appendInputJobs.put(jobId, fileInput);
		}
		distributedWork(reduceJobs, reduceWorkQueue, numRedsHost);
		if(currJobs.size() == 0){
			reduceOutputJobs.remove(jobId); // Job is finished from reduction
			for (String s : reduceJobs.keySet()){
				JobHandler jh = reduceJobs.get(s);
				if(jh!=null){
					jh.removeJobId(jobId);
					reduceJobs.put(s, jh);
				}
			}
			JobConfig conf = jobToConf.get(jobId);
			if(jobRemovalFlags.get(jobId)==1){ // If file is flagged for removal, don't let it advance
				appendInputJobs.remove(jobId);
				return;
			}
			String outputFileTemp = conf.getOutputFile() + "_temp";
			List<FileMeta> appendFiles = new ArrayList<FileMeta>();
			for(FileMeta f : appendInputJobs.get(jobId)){
				appendFiles.add(f);
			}

			try {
				FileMeta outputFinalTemp = FileIO.mergeSort(appendFiles, conf, outputFileTemp, false);
				FileMeta outputFinal = FileIO.translate(outputFinalTemp, conf);
				appendInputJobs.remove(jobId);
				finalOutputs.put(jobId, outputFinal);
				jobToConf.remove(jobId);
			} catch (Exception e){
				ExtraUtils.sendError(masterIP, e.getMessage(), jobId);
			}

		}
	}

	public synchronized void removeWorker(String ipAddr){
		if(mapJobs.containsKey(ipAddr)){
			JobHandler jh = mapJobs.get(ipAddr);
			Queue<WorkMessage> jobs = jh.getAllJobs();
			mapJobs.remove(ipAddr);
			jobs.addAll(mapWorkQueue);
			mapWorkQueue = new ConcurrentLinkedQueue<WorkMessage>();
			mapWorkQueue.addAll(jobs);
		} else{
			JobHandler jh = reduceJobs.get(ipAddr);
			Queue<WorkMessage> jobs = jh.getAllJobs();
			reduceJobs.remove(ipAddr);
			jobs.addAll(reduceWorkQueue);
			reduceWorkQueue = new ConcurrentLinkedQueue<WorkMessage>();
			reduceWorkQueue.addAll(jobs);
		}
	}

	public Set<String> getWorkers(){
		Set<String> workers= new HashSet<String>();
		workers.addAll(mapJobs.keySet());
		workers.addAll(reduceJobs.keySet());
		return workers;
	}

	private class JobHandler {
		private Map<Integer, Queue<WorkMessage>> idMessage;

		public JobHandler(){
			idMessage = new ConcurrentHashMap<Integer, Queue<WorkMessage>>();
		}

		public int getNumJobs(){
			Set<Integer> keys = idMessage.keySet();
			int sum = 0;
			for(Integer jid : keys){
				sum+=idMessage.get(jid).size();
			}
			return sum;
		}

		public void addJob(int jobId, WorkMessage m){
			if(idMessage.containsKey(jobId)){
				Queue<WorkMessage> msgs = idMessage.get(jobId);
				msgs.add(m);
				idMessage.put(jobId, msgs);
			} else{
				Queue<WorkMessage> msgs = new ConcurrentLinkedQueue<WorkMessage>();
				msgs.add(m);
				idMessage.put(jobId, msgs);
			}
		}

		public void removeJob(int jobId, String outputFile){
			Queue<WorkMessage> msgs = idMessage.get(jobId);
			if(msgs.size() == 1){
				idMessage.remove(jobId);
			}
			for(WorkMessage m : msgs){
				if(m.getOutputFile().equals(outputFile)){
					msgs.remove(m);
					idMessage.put(jobId, msgs);
					return;
				}
			}
		}
		public void removeJobId(int jobId){
			idMessage.remove(jobId);
		}
		public boolean containsJob(int jobId){
			return idMessage.keySet().contains(jobId);
		}

		public Queue<WorkMessage> getAllJobs(){
			Collection<Queue<WorkMessage>> vals =idMessage.values();
			Queue<WorkMessage> allJobs = new ConcurrentLinkedQueue<WorkMessage>();
			for(Queue<WorkMessage> q : vals){
				allJobs.addAll(q);
			}
			return allJobs;
		}
	}
}

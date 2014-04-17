package poodah.processes.listeners.master;

import java.io.IOException;
import java.util.Set;

import poodah.processes.messages.outgoing.HeartBeatMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.DistributedMap;
import poodah.processes.utils.ExtraUtils;
/**
 * The heartBeats checks whether the participants are alive. Every 2 
 * seconds a poll is sent out. If the participant is dead, we tell the master
 * and the master will try to spawn a backup if one exists. All that worker's
 * work is then redistributed
 * @author keweiz, rajagarw
 *
 */
public class HeartBeat implements Runnable{
	
	DistributedMap d;
	String workingDir;
	String myIP;
	public HeartBeat(DistributedMap d, String workingDir,
			String myIP){
		this.d = d;
		this.workingDir = workingDir;
		this.myIP = myIP;
	}
	
	@Override
	public void run() {
		while(true){
			ExtraUtils.wait(2);
			Set<String> workers = d.getWorkers();
			for(String s : workers){
				HeartBeatMessage request = new HeartBeatMessage();
				try {
					ExtraUtils.sendObject(request, s, Constants.WORKER_PORT);
				} catch (Exception e){
					d.removeWorker(s);
					String newWorker = d.getBackupWorker();
					if(newWorker != null){
						ProcessBuilder pb = new ProcessBuilder();
						pb.command(workingDir + "/StartWorker.py", myIP.toString(), 
								newWorker, workingDir);	
						try {
							pb.start();
						} catch (IOException e1) {
						}
					}
					
				}
			}
		}
	}
}

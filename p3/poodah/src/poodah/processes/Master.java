package poodah.processes;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import poodah.processes.listeners.master.AppendListener;
import poodah.processes.listeners.master.HeartBeat;
import poodah.processes.listeners.master.MasterMessageListener;
import poodah.processes.listeners.master.NewJobListener;
import poodah.processes.listeners.master.NewWorkerListener;
import poodah.processes.listeners.master.ShuffleListener;
import poodah.processes.listeners.master.TestListener;
import poodah.processes.utils.DistributedMap;
/**
 * 
 * The master launches all the listeners and all the participants
 * It uses the distributed arrays to add new workers and new jobs,
 * to merge and sort the results, and to provide fault tolerance
 * @author keweiz, rajagarw
 *
 */
public class Master {

	public static void main(String[] args) 
			throws NumberFormatException, IOException, InterruptedException{
		String myIP = InetAddress.getLocalHost().getHostAddress().toString();
		String workingDir = args[3];
		int numWorkers = Integer.parseInt(args[0]);
		int numMapsHost = Integer.parseInt(args[1]);
		int numRedsHost = Integer.parseInt(args[2]);
		// Produce some directories here. ShuffleDir gets pretty full
		String mapDir = workingDir + "/mapOut";
		String shuffleDir = workingDir + "/shuffleOut";
		String reduceDir = workingDir + "/reduceOut";
		File f = new File(mapDir); f.mkdir();
		f.setWritable(true);
		f = new File(shuffleDir); f.mkdir();
		f.setWritable(true);
		f = new File(reduceDir); f.mkdir();
		f.setWritable(true);
		DistributedMap d = 
				new DistributedMap(numWorkers, numMapsHost, numRedsHost,
						mapDir, shuffleDir, reduceDir, myIP, workingDir);
		List<Thread> threadList = new ArrayList<Thread>();
		threadList.add(new Thread(new TestListener()));
		threadList.add(new Thread(new NewWorkerListener(d)));
		threadList.add(new Thread(new NewJobListener(d)));
		threadList.add(new Thread(new ShuffleListener(d)));
		threadList.add(new Thread(new AppendListener(d)));
		threadList.add(new Thread(new MasterMessageListener(d)));
		threadList.add(new Thread(new HeartBeat(d, workingDir, myIP)));
		for (Thread t : threadList){
			t.start();
		}
		
		for(int i = 1; i < 2*(numWorkers)+1; i++){
			ProcessBuilder pb = new ProcessBuilder();
			String serverName = "";
			if(i <= 9){
				serverName = "ghc0" + i + ".ghc.andrew.cmu.edu";
			} else{
				serverName = "ghc" + i + ".ghc.andrew.cmu.edu";
			}
			pb.command(workingDir + "/StartWorker.py",myIP.toString(), serverName.toString(), workingDir);
			pb.start();
		}
		for (Thread t : threadList){
			t.join();
		}
	}
}

package poodah.processes;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;

import poodah.processes.listeners.Listener;
import poodah.processes.listeners.worker.WorkerListener;
import poodah.processes.messages.incoming.NewWorkerMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.ExtraUtils;
/**
 * The participant launches the worker listener which
 * performs the mapping and reducing
 * @author keweiz, rajagarw
 *
 */
public class Participant {

	public static void main(String[] args) throws ConnectException, IOException {
		String myIP = InetAddress.getLocalHost().getHostAddress().toString();
		String masterIP = args[0];
		Listener l = new WorkerListener(masterIP, myIP);		
		Thread t = new Thread(l);
		t.start();
		NewWorkerMessage nwm = new NewWorkerMessage();
		nwm.setWorkerIP(myIP);
		NewWorkerMessage response = (NewWorkerMessage)
			ExtraUtils.sendObject(nwm, masterIP, Constants.NEW_WORKER_PORT);
		if(!response.isWorker()){
			l.quit();
		}
		try {
			t.join();
		} catch (InterruptedException e) {
			System.err.println("InterruptedException e");
		}
	}

}

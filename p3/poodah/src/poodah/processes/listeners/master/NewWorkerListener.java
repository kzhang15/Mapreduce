package poodah.processes.listeners.master;

import poodah.processes.listeners.Listener;
import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.NewWorkerMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.DistributedMap;
import poodah.processes.utils.Strategy;

/**
 * Listener responsible for adding new workers to the framework. If the quota
 * of listeners is already reached, the listener's ip will be added to a 
 * backup list
 * @author rajagarw, keweiz
 *
 */
public class NewWorkerListener extends Listener {

	private DistributedMap d;
	static final int PORT = Constants.NEW_WORKER_PORT;

	public NewWorkerListener(DistributedMap d){
		super(PORT);
		this.d = d;
		strat = new NewWorkerStrategy();
	}

	private class NewWorkerStrategy implements Strategy {

		@Override
		public Message compute(Message in) throws IllegalArgumentException {
			NewWorkerMessage input;
			if(in == null){
				throw new IllegalArgumentException("NewWorkerMessage is null!");
			}
			if(in.getMessageType().equals(Constants.NewWorkerMessageType)){
				input = (NewWorkerMessage)in;
				boolean isWorker = d.addWorker(input.getWorkerIP());
				input.setWorker(isWorker);
				return input;
			} 
			return in;
		} 

	}

}

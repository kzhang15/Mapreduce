package poodah.processes.messages.incoming;

import poodah.processes.messages.Message;
import poodah.processes.utils.Constants;

public class NewWorkerMessage implements Message{

	private static final long serialVersionUID = -6602353279173788308L;

	private String workerIP = null;
	private boolean isWorker = false;
	@Override
	public String getMessageType() {
		return Constants.NewWorkerMessageType;
	}

	public String getWorkerIP() {
		return workerIP;
	}

	public void setWorkerIP(String workerIP) {
		this.workerIP = workerIP;
	}

	public void setWorker(boolean isWorker) {
		this.isWorker = isWorker;
	}

	public boolean isWorker() {
		return isWorker;
	}
	
}

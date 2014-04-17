package poodah.processes.messages.incoming;

import poodah.io.utils.FileMeta;
import poodah.processes.messages.Message;
import poodah.processes.utils.Constants;

public class ShuffleMessage implements Message{

	private static final long serialVersionUID = -8614617594257020634L;
	
	private String myIP;
	private int jobId;
	private FileMeta file;
	
	@Override
	public String getMessageType() {
		return Constants.ShuffleMessageType;
	}

	public FileMeta getFile() {
		return file;
	}

	public void setFile(FileMeta file) {
		this.file = file;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public String getMyIP() {
		return myIP;
	}

	public void setMyIP(String myIP) {
		this.myIP = myIP;
	}

}

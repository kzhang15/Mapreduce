package poodah.processes.messages.outgoing;

import poodah.processes.messages.Message;

public interface WorkMessage extends Message{
	public int getJobId();
	
	public String getOutputFile();
}

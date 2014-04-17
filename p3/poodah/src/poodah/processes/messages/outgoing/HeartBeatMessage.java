package poodah.processes.messages.outgoing;

import poodah.processes.messages.Message;
import poodah.processes.utils.Constants;

public class HeartBeatMessage implements Message{
	
	private static final long serialVersionUID = -6232174766190203655L;

	public String getMessageType() {
		return Constants.HeartBeatMessageType;
	}

}

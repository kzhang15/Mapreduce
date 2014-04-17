package poodah.processes.messages.outgoing;

import poodah.processes.messages.Message;
import poodah.processes.utils.Constants;

public class ProcManMessage implements Message {

	private static final long serialVersionUID = -4565721947874460826L;
	private String[] messageArr;	
	private String response;
	
	@Override
	public String getMessageType() {
		return Constants.ProcManMessageType;
	}

	public void setMessageArr(String[] messageArr) {
		this.messageArr = messageArr;
	}

	public String[] getMessageArr() {
		return messageArr;
	}

	public String getResponse() {
		return response;
	}

	public void setResponse(String response) {
		this.response = response;
	}
	
		

}

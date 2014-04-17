package poodah.processes.messages.incoming;

import poodah.processes.messages.Message;
import poodah.processes.utils.Constants;

public class TestMessage implements Message{

	private static final long serialVersionUID = -8822354923989633509L;

	private boolean response = false;
		
	@Override
	public String getMessageType() {
		return Constants.TestMessageType;
	}

	public boolean getResponse() {
		return response;
	}

	public void setResponse(boolean response) {
		this.response = response;
	}
	
}

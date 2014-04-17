package poodah.processes.messages.incoming;

import poodah.processes.messages.Message;
import poodah.processes.utils.Constants;

public class ErrorMessage implements Message{

	private static final long serialVersionUID = 6251606847819676644L;
	private String errorMessage;
	private int jobId = -1;
	
	@Override
	public String getMessageType() {
		return Constants.ErrorMessageType;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

}

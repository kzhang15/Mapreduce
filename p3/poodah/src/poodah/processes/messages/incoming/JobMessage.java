package poodah.processes.messages.incoming;

import poodah.conf.JobConfig;
import poodah.processes.messages.Message;
import poodah.processes.utils.Constants;

public class JobMessage implements Message{

	private static final long serialVersionUID = -2193462172072866490L;
	private int jobId;
	private JobConfig conf;
	
	@Override
	public String getMessageType() {
		return Constants.JobMessageType;
	}
		
	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public JobConfig getConf() {
		return conf;
	}

	public void setConf(JobConfig conf) {
		this.conf = conf;
	}
	
}
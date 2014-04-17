package poodah.processes.messages.outgoing;

import poodah.conf.JobConfig;
import poodah.io.utils.FileMeta;
import poodah.processes.messages.Message;
import poodah.processes.utils.Constants;

public class ReduceMessage implements WorkMessage{

	private static final long serialVersionUID = 5266140585695944036L;
	private int jobId;
	private FileMeta file; // RecordFileMeta
	private JobConfig conf;
	private String outputFile;
	
	@Override
	public String getMessageType() {
		return Constants.ReduceMessageType;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public FileMeta getFile() {
		return file;
	}

	public void setFile(FileMeta file) {
		this.file = file;
	}

	public JobConfig getConf() {
		return conf;
	}

	public void setConf(JobConfig conf) {
		this.conf = conf;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}
	
}

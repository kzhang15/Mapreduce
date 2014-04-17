package poodah.processes.messages.outgoing;

import poodah.conf.JobConfig;
import poodah.io.utils.FileMeta;
import poodah.processes.utils.Constants;

public class MapMessage implements WorkMessage{

	private static final long serialVersionUID = -3597687234341480851L;

	private int jobId;
	private FileMeta file;
	private int start;
	private int end;
	private JobConfig conf;
	private String outputFile;
	
	public FileMeta getFile() {
		return file;
	}

	public void setFile(FileMeta file) {
		this.file = file;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}
	
	public void setEnd(int end) {
		this.end = end;
	}

	@Override
	public String getMessageType() {
		return Constants.MapMessageType;
	}

	public JobConfig getConf() {
		return conf;
	}

	public void setConf(JobConfig conf) {
		this.conf = conf;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

}

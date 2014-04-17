package poodah.processes.listeners.master;

import java.io.IOException;
import poodah.processes.listeners.Listener;
import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.JobMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.DistributedMap;
import poodah.processes.utils.ExtraUtils;
import poodah.processes.utils.Strategy;

/**
 * Listener responsible for adding new jobs to the map-reduce framework.
 * @author rajagarw, keweiz
 *
 */
public class NewJobListener extends Listener {

	static final int PORT = Constants.NEW_JOB_PORT;
	private DistributedMap d;
	public NewJobListener(DistributedMap d){
		super(PORT);
		this.d = d;
		strat = new NewJobStrategy();
	}
	
	private class NewJobStrategy implements Strategy {

		@Override
		public Message compute(Message in) throws IllegalArgumentException {
			JobMessage input;
			if(in == null){
				throw new IllegalArgumentException("NewJobMessage is null!");
			}
			if(in.getMessageType().equals(Constants.JobMessageType)){
				input = (JobMessage)in;
				int jobId;
				try {
					jobId = d.addJob(input.getConf());
					input.setJobId(jobId);
					return input;
				} catch (InstantiationException e) {
					ExtraUtils.sendError(d.getMasterIp(), e.getMessage(), input.getJobId());
				} catch (IllegalAccessException e) {
					ExtraUtils.sendError(d.getMasterIp(), e.getMessage(), input.getJobId());
				} catch (IOException e) {
					ExtraUtils.sendError(d.getMasterIp(), e.getMessage(), input.getJobId());
				}
				return null;
			} return null;
		}
		
	}
	

}

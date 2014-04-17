package poodah.processes.listeners.master;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Set;

import poodah.processes.listeners.Listener;
import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.ErrorMessage;
import poodah.processes.messages.outgoing.ProcManMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.DistributedMap;
import poodah.processes.utils.ExtraUtils;
import poodah.processes.utils.Strategy;

/**
 * This listener is responsible for handling communication between client 
 * and server, and also handling any errors that are generated throughout
 * the framework.
 * @author rajagarw, keweiz
 *
 */
public class MasterMessageListener extends Listener{

	static final int PORT = Constants.COMM_MASTER_PORT;
	private DistributedMap d;	
	public MasterMessageListener(DistributedMap d) {
		super(PORT);
		this.d = d;
		strat = new NewMessageStrategy();
		// TODO Auto-generated constructor stub
	}

	private class NewMessageStrategy implements Strategy {

		@Override
		public Message compute(Message in) throws IllegalArgumentException {
			try{
				if(in.getMessageType().equals(Constants.ProcManMessageType)){
					ProcManMessage input = (ProcManMessage) in;
					String[] query = input.getMessageArr();
					ProcManMessage output = d.procManResponse(input, query);
					return output;

				} else if(in.getMessageType().equals(Constants.ErrorMessageType)){
					
					ErrorMessage input = (ErrorMessage)in;
					int jobId = input.getJobId();
					if(jobId > -1){
						String procManIp = d.getProcManIp(jobId);
						ExtraUtils.sendObject(input, procManIp, Constants.PROC_MAN_PORT);

					} else{ // No specific job, broadcast to all
						Set<String> procMans = d.getAllProcMans();
						for(String s : procMans){
							ExtraUtils.sendObject(input, s, Constants.PROC_MAN_PORT);
						}
					}
				}
			} catch (ConnectException e) {

			} catch (IOException e) {
				
			}
			return null;
		}

	}

}

package poodah.processes.listeners.master;

import java.io.IOException;
import java.net.ConnectException;

import poodah.processes.listeners.Listener;
import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.AppendMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.DistributedMap;
import poodah.processes.utils.ExtraUtils;
import poodah.processes.utils.Strategy;
/**
 * AppendListener is the final step in the map reduce process
 * It appends all the results together and output them
 * @author keweiz, rajagarw
 *
 */
public class AppendListener extends Listener {

	static final int PORT = Constants.APPEND_PORT;
	private DistributedMap d;
	
	public AppendListener(DistributedMap d) {
		super(PORT);
		this.d = d;
		this.strat = new AppendStrategy();
	}
	private class AppendStrategy implements Strategy {
		
		@Override
		public Message compute(Message in) throws IllegalArgumentException {
			
			AppendMessage input;
			if(in == null){
				throw new IllegalArgumentException("AppendMessage in null!");
			}
			if(in.getMessageType().equals(Constants.AppendMessageType)){
				input = (AppendMessage)in;
				try {
					d.appendFileMeta(input.getJobId(), input.getMyIP(), input.getFile());
				} catch (ConnectException e) {
					ExtraUtils.sendError(d.getMasterIp(), e.getMessage(), input.getJobId());
				} catch (IOException e) {
					ExtraUtils.sendError(d.getMasterIp(), e.getMessage(), input.getJobId());
				}
				return null;
			} 
			return null;
		}
				
	}
	
}

package poodah.processes.listeners.master;

import java.io.IOException;
import java.net.ConnectException;

import poodah.processes.listeners.Listener;
import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.ShuffleMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.DistributedMap;
import poodah.processes.utils.ExtraUtils;
import poodah.processes.utils.Strategy;

/**
 * This listener listens to the results of the map step. It will conglomerate
 * the data, and redistribute to the reducers
 * @author rajagarw, keweiz
 *
 */
public class ShuffleListener extends Listener {

	static final int PORT = Constants.SHUFFLE_PORT;
	private DistributedMap d;
	
	public ShuffleListener(DistributedMap d) {
		super(PORT);
		this.d = d;
		strat = new ShuffleStrategy();
	}
	
	private class ShuffleStrategy implements Strategy {
		@Override
		public Message compute(Message in) throws IllegalArgumentException {
			ShuffleMessage input;
			if (in == null){
				throw new IllegalArgumentException("ShuffleMessage is null!");
			}
			if(in.getMessageType().equals(Constants.ShuffleMessageType)){
				input = (ShuffleMessage)in;
				try {
					d.addShuffleJob(input.getFile(), input.getMyIP(), input.getJobId());
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

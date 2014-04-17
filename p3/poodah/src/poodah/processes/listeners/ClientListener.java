package poodah.processes.listeners;

import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.ErrorMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.Strategy;
/**
 * The client listener listens for any errors sent from the master.
 * @author keweiz
 *
 */
public class ClientListener extends Listener{

	static final int PORT = Constants.PROC_MAN_PORT;
	
	public ClientListener() {
		super(PORT);
		strat = new ClientStrategy();
	}
	private class ClientStrategy implements Strategy {

		@Override
		public Message compute(Message in) throws IllegalArgumentException {
			if(in.getMessageType().equals(Constants.ErrorMessageType)){
				ErrorMessage input = (ErrorMessage) in;
				if(input.getJobId() > -1){
					System.out.println("Job: " + input.getJobId() + 
							" has encountered an error: " + 
							input.getErrorMessage());
					
				} else {
					System.out.println("Universal Error: " + input.getErrorMessage());
				}
			}
			
			return null;
		}
		
	}
	
	
}

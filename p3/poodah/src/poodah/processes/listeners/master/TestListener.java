package poodah.processes.listeners.master;

import poodah.processes.listeners.Listener;
import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.TestMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.Strategy;

/**
 * When a ProcessManager first boots up, it sends a message here to see
 * if the framework is already running. If it doesn't receive a response,
 * it will start a framework instance.
 * @author rajagarw, keweiz
 *
 */
public class TestListener extends Listener{

	static int PORT = Constants.TEST_PORT;

	public TestListener(){
		super(PORT);
		strat = new TestStrategy();
	}
	
	private class TestStrategy implements Strategy {

		@Override
		public Message compute(Message in) throws IllegalArgumentException{
			TestMessage input;
			if(in == null){
				throw new IllegalArgumentException("TestMessage is null!");
			}
			
			if(in.getMessageType().equals(Constants.TestMessageType)){
				input = (TestMessage)in;
				input.setResponse(true);
				return input;
			} 
			return null;
		}
	}
}

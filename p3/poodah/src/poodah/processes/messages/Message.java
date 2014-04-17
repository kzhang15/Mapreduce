package poodah.processes.messages;

import java.io.Serializable;

/**
 * General interface responsible for all messages that are passed.
 * @author rajagarw, keweiz
 *
 */
public interface Message extends Serializable{
	
	/**
	 * Message types are set in a constants file. This method allows us to 
	 * know how to cast and read the information from a message.
	 * @return A method type.
	 */
	public String getMessageType();
	
}

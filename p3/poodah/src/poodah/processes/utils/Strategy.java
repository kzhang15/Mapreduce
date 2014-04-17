package poodah.processes.utils;
import poodah.processes.messages.Message;

public interface Strategy {

	public Message compute(Message in) throws IllegalArgumentException;
	
}

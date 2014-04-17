package poodah.startup;

import poodah.processes.messages.incoming.TestMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.ExtraUtils;
/**
 * This class tests whether the master is running. 
 * @author keweiz
 *
 */
public class TestMaster {

	public static void main(String[] args){
		TestMessage tm = new TestMessage();
		try{
			ExtraUtils.sendObject(tm, args[0], Constants.TEST_PORT);
			System.out.println("1");
		} catch (Exception e){
			System.out.println("0");
		}
		
	}

}

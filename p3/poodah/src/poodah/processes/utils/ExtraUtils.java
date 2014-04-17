package poodah.processes.utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;

import poodah.processes.messages.incoming.ErrorMessage;

/**
 * This class contains useful functions for us to use. 
 * This includes wait which allows us wait in the poller for 
 * 5 seconds. In addition, this useful class includes the sendObject 
 * method which allows us to send objects across sockets.
 * 
 * @author rajagarw, keweiz
 *
 */


public class ExtraUtils {
	public static void wait(int kSecs){
		long time0, time1;
		time0 = System.currentTimeMillis();
		do{
			time1 = System.currentTimeMillis();
		} while ((time1-time0) < kSecs*1000);
	}
	
	public static Object sendObject(Object o, String ip, int port) 
		throws ConnectException, IOException{
		Socket requestSocket = new Socket();
		try {
			requestSocket.connect(new InetSocketAddress(ip, port));
			ObjectOutputStream out = new ObjectOutputStream(
					requestSocket.getOutputStream());
			out.flush();
			out.writeObject(o);
			ObjectInputStream in = new ObjectInputStream(
					requestSocket.getInputStream());
			Object response = in.readObject();
			out.close();
			in.close();
			requestSocket.close();
			return response;
		
		}catch (ClassNotFoundException e) {

		}
		return null;
		
	}
	
	public static void sendError(String masterIp, String message, int jobId){
		ErrorMessage em = new ErrorMessage();
		em.setJobId(jobId);
		em.setErrorMessage(message);
		try {
			ExtraUtils.sendObject(em, masterIp, Constants.COMM_MASTER_PORT);
		} catch (ConnectException e1) {
		} catch (IOException e1) {
		}
	}
	
}

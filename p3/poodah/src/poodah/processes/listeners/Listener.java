package poodah.processes.listeners;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import poodah.processes.messages.Message;
import poodah.processes.messages.incoming.AppendMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.Strategy;

/**
 * Listener strategy pattern used here. Each listener will
 * implement this, and then implement a strategy class as well. 
 * Each listener follows the same flow -> listen, generate a response -> send back
 * @author rajagarw, keweiz
 *
 */
public abstract class Listener implements Runnable {

	private final int PORT;
	private volatile boolean quit = false;
	public Strategy strat;
	private ServerSocket sock;


	public void setStrat(Strategy strat) {
		this.strat = strat;
		return;
	}

	public Listener(int p) {
		PORT = p;
	}

	public void run() {
		try{
			sock = new ServerSocket(PORT);
			Socket clientSocket = null;
			ObjectInputStream in = null;
			ObjectOutputStream out = null;
			while(!quit) {
				try{
					clientSocket = sock.accept();
					in = new ObjectInputStream(clientSocket.getInputStream());
					out = new ObjectOutputStream(clientSocket.getOutputStream());
					Message outMessage = null;
					try {
						outMessage = strat.compute((Message)in.readObject());
					} catch (IllegalArgumentException e) {
					} catch (ClassNotFoundException e) {
					}
					out.writeObject(outMessage);	
					in.close();
					out.close();
					clientSocket.close();
				} catch(IOException e1){
					try{						
						in.close();
					}catch(Exception e2){
					}
					try{
						out.close();
					}catch(Exception e2){
					}
					try{
						clientSocket.close();
					}catch(Exception e2){
					}
				}
			}
			sock.close();
		} catch(IOException e) {
			try {
				sock.close();
			} catch (IOException e1) {
				
			}
		} 
	}

	public void quit(){
		this.quit = true;
		try {
			sock.close();
		} catch (IOException e) {

		}

	}

}

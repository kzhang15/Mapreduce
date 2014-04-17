package poodah.startup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import poodah.processes.listeners.ClientListener;
import poodah.processes.listeners.Listener;
import poodah.processes.messages.Message;
import poodah.processes.messages.outgoing.ProcManMessage;
import poodah.processes.utils.Constants;
import poodah.processes.utils.ExtraUtils;

/**
 * ProcessManager produces an interactive shell for the user. It allows
 * the user to send new job(s), kill job, monitor job, and stop the system
 * @author keweiz, rajagarw
 *
 */
public class ProcessManager {

	private static String root_dir;
	private static String masterIP;
	private static String xmlConfig_path;
	
	public static void main (String[] args) {
		Listener l = new ClientListener();
		Thread t = new Thread(l);
		try {
			xmlConfig_path = args[0];
			xmlParser(xmlConfig_path);
			startMaster(root_dir,xmlConfig_path);
			t.start();
		} catch (IOException e) {
			System.err.println("Cannot start system");
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
	    } catch (SAXException e) {
		    e.printStackTrace();
		}
	    while (true) {
	    	System.out.print(">");
	        BufferedReader br = new BufferedReader(
	        		new InputStreamReader(System.in));
	        try {
	        	String read = br.readLine().trim();
	        	if (read == null || read.length() == 0)
	        		continue;
	        	else {
	        		String[] tokens = read.split(" ");
	        		if (tokens[0].equalsIgnoreCase("quit") && tokens.length == 1){
	        			l.quit();
	        			t.join();
	        			return;
	        		} else if(tokens[0].equalsIgnoreCase("help") && tokens.length == 1)
	        			help();
	        		else if(tokens[0].equalsIgnoreCase("stop") && tokens.length == 1) {
	        			ProcManMessage m = new ProcManMessage();
	        			String [] strArr = new String[2];
	        			strArr[0] = tokens[0];
	        			strArr[1] = xmlConfig_path;
	        			m.setMessageArr(strArr);
	        			ExtraUtils.sendObject((Message)m, masterIP, 
	        					Constants.COMM_MASTER_PORT);
	        		}
	        		else if (tokens[0].equalsIgnoreCase("killJob") && tokens.length == 2) {
	        			ProcManMessage m = new ProcManMessage();
	        			m.setMessageArr(tokens);
	        			ExtraUtils.sendObject((Message)m, masterIP, 
	        					Constants.COMM_MASTER_PORT);
	        			
	        		}
	        		else if (tokens[0].equalsIgnoreCase
	        				("startJob") && tokens.length > 1) {
	        			startJobs(tokens);

	        		}
	        		else if (tokens[0].equals("monitor") && tokens.length == 2) {
	        			ProcManMessage m = new ProcManMessage();
	        			m.setMessageArr(tokens);
	        			ExtraUtils.sendObject((Message)m, masterIP, 
	        					Constants.COMM_MASTER_PORT);
	        			ProcManMessage rm = (ProcManMessage) ExtraUtils.sendObject((Message)m, masterIP, 
	        					Constants.COMM_MASTER_PORT);
	        			System.out.println(rm.getResponse());
	        		}
	        		else {
	        			System.err.println("error trying to read your input!");
	        		}
	        	}
	        }
	        catch (Exception e) {     	
	        	System.err.println(e.getMessage());
	         }
	    }  
	}
	
	private static void help() {
		System.out.println("startJob [full path of jar file] [full path of jar file]... -send the job request(s) to the master and get a job id in return");
		System.out.println("stop -kill the master, all the particpants, and jobs");
		System.out.println("help -list all the possible commands " +
				"for the process manager");
		System.out.println("killjob [jobid] -kill the job specific to" +
				"the provided job id");
		System.out.println("quit -quit the process manager");
		System.out.println("monitor [jobid] -return information about " +
				"the job");
		return;
	}
	
	private static void startMaster(String path, String xmlConfig) throws IOException {
		ProcessBuilder pb = new ProcessBuilder();
		pb.command(path + "/StartMaster.py", xmlConfig);
		Process proc = pb.start();
		Reader reader = new InputStreamReader(proc.getInputStream());
		 int ch;
	        while ((ch = reader.read()) != -1)
	            System.err.print((char) ch);
	        reader.close();
	}
	
	private static void xmlParser(String s) throws ParserConfigurationException, 
	SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(s);
		NodeList List = doc.getElementsByTagName("data");
		Element ListTest = (Element)List.item(3);
		Element ListTest2 = (Element)List.item(4);
		root_dir = ListTest.getAttribute("root_dir");
		masterIP = ListTest2.getAttribute("master_server");
	}
	

	private static void startJobs(String[] s) throws Exception {
		ProcessBuilder pb = new ProcessBuilder();
		pb.redirectErrorStream(true);
		for (int i = 1; i <s.length; i++) {
			pb.command("java","-jar",s[i]);
			Process proc = pb.start();
			Reader reader = new InputStreamReader(proc.getInputStream());
			int ch;
			while ((ch = reader.read()) != -1)
				System.err.print((char) ch);
			reader.close();
		}		
		return;
	}
}

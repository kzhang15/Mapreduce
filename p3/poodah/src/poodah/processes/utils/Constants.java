package poodah.processes.utils;

public interface Constants {

	public static final int TEST_PORT = 1073;
	public static final int NEW_JOB_PORT = 1074;
	public static final int NEW_WORKER_PORT = 1075;
	public static final int SHUFFLE_PORT = 1076;
	public static final int APPEND_PORT = 1077;
	public static final int WORKER_PORT = 1078;
	public static final int PROC_MAN_PORT = 1079; // Listener on client for errors
	public static final int COMM_MASTER_PORT = 1080;
	
	public static final String TestMessageType = "TestMessage";
	public static final String NewWorkerMessageType = "NewWorkerMessage";
	public static final String JobMessageType = "JobMessage";
	public static final String ShuffleMessageType = "ShuffleMessage";
	public static final String AppendMessageType = "AppendMessage";
	public static final String HeartBeatMessageType = "HearBeatMessage";
	public static final String MapMessageType = "MapMessage";
	public static final String ReduceMessageType = "ReduceMessage";
	public static final String ErrorMessageType = "ErrorMessage";
	public static final String ProcManMessageType = "ProcManMessage";
	
	public static final String TextFileMetaType = "TextFileMeta";
	public static final String ByteFileMetaType = "ByteFileMeta";
	public static final String RecordFileMetaType = "RecordFileMeta";
}
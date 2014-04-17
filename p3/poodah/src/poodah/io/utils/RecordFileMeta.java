package poodah.io.utils;

import poodah.processes.utils.Constants;
/**
 * RecordFileMeta provides a way to represent a record file
 * @author keweiz
 *
 */
public class RecordFileMeta implements FileMeta{
	private static final long serialVersionUID = -3646601267224025177L;
	private String fileName;
	private int numRecords;
	private int startPos;
	private int endPos;
	
	
	public int getStartPos() {
		return startPos;
	}

	public void setStartPos(int startPos) {
		this.startPos = startPos;
	}

	public int getEndPos() {
		return endPos;
	}

	public void setEndPos(int endPos) {
		this.endPos = endPos;
	}

	@Override
	public String getFileType() {
		return Constants.RecordFileMetaType;
	}

	@Override
	public String getFileName() {
		return fileName;
	}

	@Override
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public int getSize() {
		return numRecords;
	}

	@Override
	public void setSize(int size) {
		this.numRecords = size;
	}
	

	
}

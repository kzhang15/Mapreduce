package poodah.io.utils;

import poodah.processes.utils.Constants;

public class TextFileMeta implements FileMeta{
	
	private static final long serialVersionUID = -6448918927367654297L;
	private String fileName;
	private int numLines;

	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public int getSize() {
		return numLines;
	}
	public void setSize(int size) {
		this.numLines = size;
	}
	@Override
	public String getFileType() {
		return Constants.TextFileMetaType;
	}

}

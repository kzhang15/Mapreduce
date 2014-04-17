package poodah.io.utils;

import java.io.Serializable;

/**
 * File meta is a representation of a file
 * @author keweiz, rajagarw
 *
 */
public interface FileMeta extends Serializable{
	

	/**
	 * File types held in constants file.
	 * @return
	 */
	public String getFileType();
	
	/**
	 * A filename associated with the file meta
	 * @return - Filename
	 */
	public String getFileName();
	
	/**
	 * Set the filename of this FileMeta
	 * @param fileName
	 */
	public void setFileName(String fileName);
	
	/**
	 * Get the size of the file meta. This can be interpreted as the number
	 * of lines or the number of bytes. It depends on the implementing subclass.
	 * @return
	 */
	public int getSize();
	
	/**
	 * Set the size.
	 * @param size
	 */
	public void setSize(int size);
	
}

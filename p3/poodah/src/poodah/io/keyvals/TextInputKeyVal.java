package poodah.io.keyvals;

public class TextInputKeyVal implements KeyVal<Integer, String>{

	private static final long serialVersionUID = 7681720258356617687L;
	private Integer key;
	private String val;
	@Override
	public void setKey(Integer k) {
		this.key = k;
	}
	@Override
	public Integer getKey() {
		return this.key;
	}
	@Override
	public void setValue(String v) {
		this.val = v;
	}
	@Override
	public String getValue() {
		return this.val;
	}

	
}

package poodah.io.keyvals;

public class TextIntOutputKeyVal implements KeyVal<String, Integer>{

	private static final long serialVersionUID = 7681720258356617687L;
	private String key;
	private Integer val;

	public String getKey() {
		return key;
	}
	public void setKey(String k) {
		this.key = k;
	}
	public Integer getValue() {
		return val;
	}
	public void setValue(Integer v) {
		this.val = v;
	}

}

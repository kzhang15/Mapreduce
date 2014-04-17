package poodah.io.keyvals;

public class IntTextOutputKeyVal implements KeyVal<Integer, String>{

	private static final long serialVersionUID = 5825740652473549033L;
	private Integer key;
	private String value;
	
	@Override
	public void setKey(Integer k) {
		this.key = k;		
	}

	@Override
	public Integer getKey() {
		return key;
	}

	@Override
	public void setValue(String v) {
		this.value = v;
	}

	@Override
	public String getValue() {
		return this.value;
	}

}

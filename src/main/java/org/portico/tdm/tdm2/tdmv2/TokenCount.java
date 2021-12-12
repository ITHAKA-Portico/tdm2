package org.portico.tdm.tdm2.tdmv2;

public class TokenCount {
	
	String token;
	
	int count;
	
	public TokenCount(String t, int count) {
		this.token = t;
		this.count = count;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}

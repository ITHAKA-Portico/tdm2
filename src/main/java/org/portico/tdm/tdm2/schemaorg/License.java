package org.portico.tdm.tdm2.schemaorg;

public class License {
	
	String type;
	
	String url;
	
	String statement;
	
	public License() {
		
	}
	
	public License(String type, String url, String statement) {
		this.type = type;
		this.url = url;
		this.statement = statement;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getStatement() {
		return statement;
	}

	public void setStatement(String statement) {
		this.statement = statement;
	}

}

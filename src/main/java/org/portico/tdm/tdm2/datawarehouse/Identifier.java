package org.portico.tdm.tdm2.datawarehouse;


import org.portico.tdm.tdm2.tools.TDMUtil;

public class Identifier {
	
	String name;				//lower case id name, ie oclc, isbn, issn, doi
	String value;
	
	public Identifier() {
		
	}
	
	public Identifier(String idType, String value) throws Exception {
		
		if ( idType == null || idType.isEmpty() ) {
			throw new Exception("Error Identifier constructor parameter");
		}
		
		idType = idType.toLowerCase();
		String idValue = value;
		
		if ( idType.equalsIgnoreCase("issn")) {
			try {
				idValue = TDMUtil.formatISSN(value);
			} catch (Exception e) {
				throw new Exception( "Error formatting issn number " + value);
			}
		}
		else if ( idType.equalsIgnoreCase("isbn")) {
			try {
				idValue = TDMUtil.formatISBN(value);
			} catch (Exception e) {
				throw new Exception( "Error formatting isbn number " + value);
			}
		}
		
		
		setName(idType);
		setValue( idValue );
	}
	
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}

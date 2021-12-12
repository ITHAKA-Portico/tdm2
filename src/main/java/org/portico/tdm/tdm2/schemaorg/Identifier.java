package org.portico.tdm.tdm2.schemaorg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.tools.TDMUtil;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Identifier {
	
	@JsonProperty("@type")
	String type;
	String propertyID;  	//"OCoLC", "isbn", "issn"
	String value;
	
	//other members
	static Logger logger = LogManager.getLogger(Identifier.class.getName());
	static String programName = "Identifier";
	
	public Identifier() {
		
	}
	
	public Identifier(String idType, String value)  {
		
		this.type = "PropertyValue";
		this.propertyID = idType;
		
		if ( idType.equalsIgnoreCase("issn")) {
			try {
				this.value = TDMUtil.formatISSN(value);
			} catch (Exception e) {
				logger.error( programName +":Error formatting issn number " + value);
			}
		}
		else if ( idType.equalsIgnoreCase("isbn")) {
			try {
				this.value = TDMUtil.formatISBN(value);
			} catch (Exception e) {
				logger.error( programName +":Error formatting isbn number " + value);
			}
		}
		else {
			this.value = value;
		}
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getPropertyID() {
		return propertyID;
	}

	public void setPropertyID(String propertyID) {
		this.propertyID = propertyID;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	

}

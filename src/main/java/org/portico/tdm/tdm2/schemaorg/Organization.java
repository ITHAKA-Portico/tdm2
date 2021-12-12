package org.portico.tdm.tdm2.schemaorg;

import org.portico.tdm.tdm2.tools.TDMUtil;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Organization {
	
	@JsonProperty("@id")
	String id;
	
	String name;
	
	@JsonProperty("@type")
	String type;
	
	@JsonProperty("address")
	Address address;
	
	public Organization() {
		
	}
	

	public Organization(String code, String name) throws Exception {
		this.type = "Organization";
		this.name = name;
		
		if ( code.equalsIgnoreCase("Portico")) {
			this.id = "http://portico.org/";
		}
		else {
			this.id = TDMUtil.getPublisherWebsiteUrl( code );	
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

}

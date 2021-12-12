package org.portico.tdm.tdm2.schemaorg;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Address {
	@JsonProperty("@id")
	String id;
	
	String streetAddress;
	
	@JsonProperty("addressCountry")
	String country;
	
	@JsonProperty("postalCode")
	String postalCode;
	
	@JsonProperty("@type")
	String type;		//PostalAddress
	
	public Address() {
		
	}
	
	public Address(String steetaddress) {
		this.streetAddress = steetaddress;
		this.type="PostalAddress";
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getStreetAddress() {
		return streetAddress;
	}

	public void setStreetAddress(String streetAddress) {
		this.streetAddress = streetAddress;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}

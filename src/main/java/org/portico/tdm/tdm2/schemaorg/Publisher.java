package org.portico.tdm.tdm2.schemaorg;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.portico.tdm.tdm2.tools.TDMUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Publisher {
	
	@JsonProperty("@id")
	String id;			//publisher's website url, read from cmi_providier table.
	
	String name;		//publisher name
	
	@JsonIgnore
	String publisherIdCode;		//CAMBRIDGE
	
	@JsonProperty("@type")
	String type;
	
	@JsonProperty("address")
	String address;				//not in cmi_providers table
	
	@JsonProperty("country")
	String country;			//in CMI_PROVIDERs table
	
	public Publisher() {
		
	}

	public Publisher(String publisher_id_code) throws Exception {
		this.publisherIdCode = publisher_id_code;
		this.type = "Organization";

		populatePublisher();
		
	}
	
	public Publisher(String publisherName, String idURL) {
		this.name = publisherName;
		this.id = idURL;
		this.type="Organization";
	}


	private void populatePublisher(  ) throws Exception {
		
		
		String query1 = "select * from cmi_providers where provider_id='" + getPublisherIdCode() + "'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			
			if ( rs.next()) {
				String provider_name = rs.getString("provider_name");
				String website_url = rs.getString("website_url");
				String country = rs.getString("country");
				setName( provider_name );
				setId( website_url );
				setCountry(country);
			}
			else {
				throw new Exception("Cannot locate publisher with id " + getId());
			}
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
	}
	

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public String getPublisherIdCode() {
		return publisherIdCode;
	}

	public void setPublisherIdCode(String publisherIdCode) {
		this.publisherIdCode = publisherIdCode;
	}


	public String getAddress() {
		return address;
	}


	public void setAddress(String address) {
		this.address = address;
	}


	public String getCountry() {
		return country;
	}


	public void setCountry(String country) {
		this.country = country;
	}


}

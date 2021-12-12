package org.portico.tdm.tdm2.schemaorg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Person {

	@JsonProperty("@id")
	String id;
	String firstName;
	String lastName;
	@JsonProperty("@type")
	String type;
	String fullName;
	@JsonIgnore
	String articleId;	//only for hashCode purpose
	
	String porticoPrefix = "http://portico.org/";
	
	public Person() {
		
	}
	
	public Person(String firstName, String lastName, String auId ) {
		this.firstName = firstName.trim();
		this.lastName = lastName.trim();
		this.fullName = this.firstName + " " + this.lastName;
		this.type="Person";
		this.articleId = auId;
		this.id = porticoPrefix + this.hashCode();
	}
	
	public Person(String name, String auId) {
		this.fullName = name.trim();
		this.type = "Person";
		this.articleId = auId;
		this.id = porticoPrefix + this.hashCode();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((articleId == null) ? 0 : articleId.toLowerCase().hashCode());
		result = prime * result + ((firstName == null) ? 0 : firstName.toLowerCase().hashCode());
		result = prime * result + ((fullName == null) ? 0 : fullName.toLowerCase().hashCode());
		result = prime * result + ((lastName == null) ? 0 : lastName.toLowerCase().hashCode());
		result = prime * result + ((type == null) ? 0 : type.toLowerCase().hashCode());
		return result;
	}



}

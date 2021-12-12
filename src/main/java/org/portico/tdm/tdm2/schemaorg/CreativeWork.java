package org.portico.tdm.tdm2.schemaorg;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.As.PROPERTY;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME;

//AWSOps needs this annotation to work. JsonLDGenerator needs to comment out this part.
/*
@JsonTypeInfo(use = NAME, include = PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value=Book.class, name = "Book"),
  @JsonSubTypes.Type(value=Chapter.class, name = "Chapter"),
  @JsonSubTypes.Type(value=Periodical.class, name = "Periodical"),
  @JsonSubTypes.Type(value=PublicationVolume.class, name = "PublicationVolume"),
  @JsonSubTypes.Type(value=PublicationIssue.class, name = "PublicationIssue"),
  @JsonSubTypes.Type(value=Article.class, name = "Article")
}) */
public class CreativeWork {
	
	@JsonIgnore   //For referencing as URI, two slashes - ark://27927/pf205c4sgk
	String id;

	@JsonIgnore
	String type;
	
	public CreativeWork() {
		
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}

package org.portico.tdm.tdm2.tdmv2;

import java.util.List;
import java.util.Map;

import org.portico.tdm.tdm2.datawarehouse.Identifier;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * This is the general TDMV2 json object. It follows metadata.schema.json structure.
 * @author dxie
 *
 */
@JsonPropertyOrder({"id", "title", "subTitle", "docType", 
					"docSubType", 	//added 12/8/2021
					"provider", "url", "creator", "publicationYear", "datePublished", 
					"doi", "publisher", "placeOfPublication", "collection", "isPartOf", "hasPartTitle", "dateModified",	"identifier", "volumeNumber", "issueNumber", 
					"sequence", "pageStart", "pageEnd", "pagination", "pageCount", "wordCount", "language", "abstract", 
					"sourceCategory", "tdmCategory", "outputFormat", "unigramCount", "fullText" })
public class TDMV2JsonObject {
	
	@JsonProperty("id")
	String arkId;				//2 slashes comparing to auid.
	
	@JsonIgnore
	String auid;				//au_id of article, book, or su_id of chapter
	
	String title;
	
	String subTitle;

	String docType;				//article, book, or chapter
	
	String docSubType;			//added 12/8/2021. current only for article type
	
	int publicationYear;
	
	String placeOfPublication;
	
	String provider;			//portico
	
	String doi;
	
	String datePublished;		//Date in ISO 8601 format, YYYY-MM-DD
	
	String url;					//URL for web version of document
	
	List<String> creator;		//URL for web version of document
	
	List<String> collection;	//Collection information as identified by the source
	
	String pageStart;
	
	String pageEnd;
	
	int pageCount;
	
	int wordCount;				//token count
	
	String pagination;	
	
	List<String> language;		//Languages found in document normalized to IETF BCP 47. Array???
	
	String publisher;
	
	@JsonProperty("abstract")
	String abstractStr;
	
	String isPartOf;			//Container title. Usually book or journal title.
	
	List<String> hasPartTitle;	//Title of sub-items, e.g. book chapters when the described document is a book.
	
	@JsonProperty("identifier")
	List<Identifier> identifiers;		//array of name->value pair, name: lowercased identifier name
	
	List<String> sourceCategory;	//Category/discipline/subjects assigned by provider
	
	String tdmCategory;			//will be added by Ted's program later
	
	int sequence;				//Article or chapter sequence if available.
	
	String issueNumber;
	
	String volumeNumber;
	
	List<String> outputFormat;					//"unigram", "bigram", "trigram", "fullText"
	
	Map<String, Integer> unigramCount;			//word frequency map
	
	List<String> fullText;						//full text by page string list
	
	String dateModified;				//date-time, Date this document was modified

	@JsonIgnore
	String contentSetName;
	
	@JsonIgnore
	boolean fullTextAvailable;					//Is document open access or not

	public TDMV2JsonObject() {
		
	}


	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getSubTitle() {
		return subTitle;
	}

	public void setSubTitle(String subTitle) {
		this.subTitle = subTitle;
	}

	public String getDocType() {
		return docType;
	}

	public void setDocType(String docType) {
		this.docType = docType;
	}

	public int getPublicationYear() {
		return publicationYear;
	}

	public void setPublicationYear(int publicationYear) {
		this.publicationYear = publicationYear;
	}

	public String getProvider() {
		return provider;
	}

	public void setProvider(String provider) {
		this.provider = provider;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public String getDatePublished() {
		return datePublished;
	}

	public void setDatePublished(String datePublished) {
		this.datePublished = datePublished;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getPageStart() {
		return pageStart;
	}

	public List<Identifier> getIdentifiers() {
		return identifiers;
	}


	public void setIdentifiers(List<Identifier> identifiers) {
		this.identifiers = identifiers;
	}


	public void setPageStart(String pageStart) {
		this.pageStart = pageStart;
	}

	public String getPageEnd() {
		return pageEnd;
	}

	public void setPageEnd(String pageEnd) {
		this.pageEnd = pageEnd;
	}

	public int getPageCount() {
		return pageCount;
	}

	public void setPageCount(int pageCount) {
		this.pageCount = pageCount;
	}

	public int getWordCount() {
		return wordCount;
	}

	public void setWordCount(int wordCount) {
		this.wordCount = wordCount;
	}

	public String getPagination() {
		return pagination;
	}

	public void setPagination(String pagination) {
		this.pagination = pagination;
	}


	public Map<String, Integer> getUnigramCount() {
		return unigramCount;
	}

	public void setUnigramCount(Map<String, Integer> unigramCount) {
		this.unigramCount = unigramCount;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}

	public String getAbstractStr() {
		return abstractStr;
	}

	public void setAbstractStr(String abstractStr) {
		this.abstractStr = abstractStr;
	}

	public String getIsPartOf() {
		return isPartOf;
	}

	public void setIsPartOf(String isPartOf) {
		this.isPartOf = isPartOf;
	}

	public List<String> getHasPartTitle() {
		return hasPartTitle;
	}

	public void setHasPartTitle(List<String> hasPartTitle) {
		this.hasPartTitle = hasPartTitle;
	}



	public List<String> getSourceCategory() {
		return sourceCategory;
	}

	public void setSourceCategory(List<String> sourceCategory) {
		this.sourceCategory = sourceCategory;
	}

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}

	public String getIssueNumber() {
		return issueNumber;
	}

	public void setIssueNumber(String issueNumber) {
		this.issueNumber = issueNumber;
	}

	public String getVolumeNumber() {
		return volumeNumber;
	}

	public void setVolumeNumber(String volumeNumber) {
		this.volumeNumber = volumeNumber;
	}

	public String getTdmCategory() {
		return tdmCategory;
	}

	public void setTdmCategory(String tdmCategory) {
		this.tdmCategory = tdmCategory;
	}

	public String getArkId() {
		return arkId;
	}

	public void setArkId(String arkId) {
		this.arkId = arkId;
	}

	public String getAuid() {
		return auid;
	}

	public void setAuid(String auid) {
		this.auid = auid;
	}

	public String getContentSetName() {
		return contentSetName;
	}

	public void setContentSetName(String contentSetName) {
		this.contentSetName = contentSetName;
	}

	public List<String> getLanguage() {
		return language;
	}


	public void setLanguage(List<String> language) {
		this.language = language;
	}


	public boolean isFullTextAvailable() {
		return fullTextAvailable;
	}


	public void setFullTextAvailable(boolean fullTextAvailable) {
		this.fullTextAvailable = fullTextAvailable;
	}


	public List<String> getOutputFormat() {
		return outputFormat;
	}


	public void setOutputFormat(List<String> outputFormat) {
		this.outputFormat = outputFormat;
	}


	public List<String> getFullText() {
		return fullText;
	}


	public void setFullText(List<String> fullText) {
		this.fullText = fullText;
	}


	
	public List<String> getCreator() {
		return creator;
	}


	public void setCreator(List<String> creator) {
		this.creator = creator;
	}


	public String getPlaceOfPublication() {
		return placeOfPublication;
	}


	public void setPlaceOfPublication(String placeOfPublication) {
		this.placeOfPublication = placeOfPublication;
	}


	public String getDateModified() {
		return dateModified;
	}


	public void setDateModified(String dateModified) {
		this.dateModified = dateModified;
	}


	public List<String> getCollection() {
		return collection;
	}


	public void setCollection(List<String> collection) {
		this.collection = collection;
	}


	public String getDocSubType() {
		return docSubType;
	}


	public void setDocSubType(String docSubType) {
		this.docSubType = docSubType;
	}

	
	
}

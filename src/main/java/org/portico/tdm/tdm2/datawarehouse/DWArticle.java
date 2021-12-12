package org.portico.tdm.tdm2.datawarehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.tools.TDMUtil;

public class DWArticle {
	
	//members from Analytics DB

	String content_set_name;						//also issn_no
	String vol_no;
	String issue_no;
	String au_id;
	String journal_title;
	String article_title;
	String article_creator;
	String sort_key;
	String display_label;
	String pub_year;
	Date pub_date;
	Timestamp timestamp;
	
	int article_seq;
	String start_page_no;
	String end_page_no;
	String page_ranges;
	int no_of_pages;
	String duplication_flag;
	String dup_of_article_AUID;
	
	//other fields from archive DB
	String provider;
	String language;
	String source;
	String format;
	String rights;
	String publisher;
	List<String> issn;
	String isbn;
	String doi;
	String pii;
	String oclc;
	String url;
	String institution_article_id;  //cannot find one.  a_au_dmd_identifier.pmd_type='publisher-id' 
									//and pmd_source_elem='article-id' has lots of duplicate pmd_identifier values
	

	DWIssue parent_issue;
	String journal_ark_id;
	String issue_ark_id;
	
	//other members
	static Logger logger = LogManager.getLogger(DWArticle.class.getName());
	static String programName = "DWArticle";
	
	public DWArticle(String auid) {
		
		this.au_id = auid;
	}

	/**
	 * This method query Analytics DB and get DW_Article information
	 * @throws Exception 
	 */
	public void populateDWArticle() throws Exception {
		String article_query = "select * from dw_article where au_id='" + getAu_id() + "'";
		String dmd_query = "select * from a_au_dmd where a_au_id='" + getAu_id() + "'";
		String issue_id_query = "select ark_id from dw_issue where issue_id =?";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();
				PreparedStatement pstmt = conn.prepareCall(issue_id_query); ) {
			
			ResultSet rs = stmt.executeQuery(article_query);
			int issue_id;
			
			if ( rs.next()) {
				issue_id = rs.getInt("issue_id");
				content_set_name = rs.getString("issn_no");
				vol_no = rs.getString("vol_no");
				issue_no = rs.getString("issue_no");
				journal_title = rs.getString("journal_title");
				article_title = rs.getString("article_title");
				article_creator = rs.getString("article_creator");
				sort_key = rs.getString("sort_key");
				display_label = rs.getString("display_label");
				pub_year = rs.getString("pub_year");
				pub_date = rs.getDate("pub_date");
				timestamp = rs.getTimestamp("timestamp");
				
				article_seq = rs.getInt("article_seq");
				start_page_no = rs.getString("start_page_no");
				end_page_no = rs.getString("end_page_no");
				page_ranges = rs.getString("page_range");
				no_of_pages = rs.getInt("no_of_pages");
				duplication_flag = rs.getString("duplication_flag");
				dup_of_article_AUID = rs.getString("dup_of_article_auid");
				
				if ( page_ranges != null && page_ranges.indexOf("-") == -1 && page_ranges.indexOf(",") == -1 ) {
					if ( start_page_no != null && end_page_no != null && start_page_no.equals(end_page_no)) {
						page_ranges = "pp. " + start_page_no;
					}
					else {
						page_ranges = "pp. " + start_page_no + "-" + end_page_no;
					}
				}
				
			}
			else {
				logger.error( programName + ":populateDWArticle:Cannot find article with au_id:" + getAu_id());
				throw new Exception("Cannot find article with auid");
			}
			
			rs = stmt.executeQuery(dmd_query);
			if ( rs.next() ) {
				language = rs.getString("pmd_language");
				source = rs.getString("pmd_source");
				format = rs.getString("pmd_format");
				rights = rs.getString("pmd_rights");
				publisher = rs.getString("pmd_publisher");
			}
			else {
				logger.error( programName + ":populateDWArticle:Cannot find a_au_dmd with au_id:" + getAu_id());
				throw new Exception("Cannot find article with auid");
			}
			
			pstmt.setInt(1, issue_id);
			
			rs = pstmt.executeQuery();
			if ( rs.next()) {
				issue_ark_id = rs.getString("ark_id");
			}
			else {
				logger.error( programName + ":populateDWArticle:Cannot find issue ark id with au_id:" + getAu_id());
				throw new Exception("Cannot find issue ark id with auid");
			}
			rs.close();

		}
		catch(Exception e) {
			throw new Exception( au_id + " " + e.getMessage());
		}
		
		try {
			populateArticleIdentifiers();
		}
		catch(Exception e) {
			throw new Exception( au_id + " " + e.getMessage());
		}

	}
	
	public void populateArticleIdentifiers() throws Exception {
		
		String au_id = getAu_id();
		
		String identifier_query = "select * from a_au_dmd_identifier where a_au_id='" + au_id + "'";
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(identifier_query);
			
			String pmdType, idValue, elem_source;
			Set<String> issns = new HashSet<>();
			
			while ( rs.next() ) {
				pmdType = rs.getString("pmd_type");
				elem_source = rs.getString("pmd_source_elem");

				switch(pmdType) {
				case "oclc":	//pmd_source_elem='book-id' only so article won't have oclc
					idValue = rs.getString("pmd_identifier");
					if ( oclc == null ) { oclc = idValue; }
					else { oclc += ", " + idValue; }
					break;
				case "issn":
					idValue = rs.getString("pmd_identifier");
					issns.add(idValue);
					break;
				case "isbn":
					//only take pmd_source_elem='article-id'
					if ( elem_source != null && elem_source.equalsIgnoreCase("article-id")) {
						idValue = rs.getString("pmd_identifier");
						if ( isbn == null ) { isbn = idValue; }
						else { isbn += ", " + idValue; }
					}
					break;
				case "pii":
					if ( elem_source != null && elem_source.equalsIgnoreCase("article-id")) {
						idValue = rs.getString("pmd_identifier");
						if ( pii == null ) { pii = idValue; }
						else { pii += ", " + idValue; }
					}
					break;
				case "doi":
					if ( elem_source != null && elem_source.equalsIgnoreCase("article-id")) {
						idValue = rs.getString("pmd_identifier");
						if ( doi == null ) { doi = idValue; }		//only take 1 doi
					}
					break;
				case "url":	case "URL": //probably only D-Collection has url/URL
					idValue = rs.getString("pmd_identifier");
					if ( url == null ) { url = idValue; }
					else { url += ", " + idValue; }
					break;
				case "publisher-id":
					if ( elem_source != null && elem_source.equalsIgnoreCase("article-id")) {
						idValue = rs.getString("pmd_identifier");
						if ( institution_article_id == null ) { institution_article_id = idValue; }		
						else { institution_article_id += ", " + idValue; }
					}
					break;
				default:
				}
				
			}
			
		
			issn = new ArrayList<>();
			issn.addAll( issns );
			
		
			rs.close();
		}
		catch(Exception e) {
			throw new Exception( au_id + " " + e.getMessage());
		}
	}


	/**
	 * update pub_year, pub_date in dw_article table
	 * @param conn
	 * @throws Exception 
	 */
	public void updateDateInDB(Connection conn) throws Exception {
		String pub_year = getPub_year();
		Date pub_date = getPub_date();
		String csname = getContent_set_name();
		String volNo = getVol_no();
		String issueNo = getIssue_no();
		String au_id = getAu_id();
		
		String query = "update dw_article set pub_year=?, pub_date=? where issn_no=? and vol_no=? and issue_no=? and au_id=?";
		try ( PreparedStatement stmt = conn.prepareStatement(query); ) {
			stmt.setString(1, pub_year);
			stmt.setDate(2, new java.sql.Date(pub_date.getTime()));
			stmt.setString(3, csname);
			stmt.setString(4, volNo);
			stmt.setString(5, issueNo);
			stmt.setString(6, au_id);
			
			stmt.executeQuery();
			
		}
		catch(Exception e) {
			logger.error( programName + ":updateDateInDB " + csname + " " + volNo + " " + issueNo + " " + au_id + " " + e);
			throw e;
		}
		
	}


	


	public String getContent_set_name() {
		return content_set_name;
	}

	public void setContent_set_name(String issn_no) {
		this.content_set_name = issn_no;
	}

	public String getVol_no() {
		return vol_no;
	}

	public void setVol_no(String vol_no) {
		this.vol_no = vol_no;
	}

	public String getIssue_no() {
		return issue_no;
	}

	public void setIssue_no(String issue_no) {
		this.issue_no = issue_no;
	}

	public String getAu_id() {
		return au_id;
	}

	public void setAu_id(String au_id) {
		this.au_id = au_id;
	}

	public String getJournal_title() {
		return journal_title;
	}

	public void setJournal_title(String journal_title) {
		this.journal_title = journal_title;
	}

	public String getArticle_title() {
		return article_title;
	}

	public void setArticle_title(String article_title) {
		this.article_title = article_title;
	}

	public String getSort_key() {
		return sort_key;
	}

	public void setSort_key(String sort_key) {
		this.sort_key = sort_key;
	}

	public String getDisplay_label() {
		return display_label;
	}

	public void setDisplay_label(String display_label) {
		this.display_label = display_label;
	}

	public String getPub_year() {
		return pub_year;
	}

	public void setPub_year(String pub_year) {
		this.pub_year = pub_year;
	}

	public Date getPub_date() {
		return pub_date;
	}

	public void setPub_date(Date pub_date) {
		this.pub_date = pub_date;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public int getArticle_seq() {
		return article_seq;
	}

	public void setArticle_seq(int article_seq) {
		this.article_seq = article_seq;
	}

	public String getInstitution_article_id() {
		return institution_article_id;
	}

	public void setInstitution_article_id(String institution_article_id) {
		this.institution_article_id = institution_article_id;
	}

	public String getStart_page_no() {
		return start_page_no;
	}

	public void setStart_page_no(String start_page_no) {
		this.start_page_no = start_page_no;
	}

	public String getEnd_page_no() {
		return end_page_no;
	}

	public void setEnd_page_no(String end_page_no) {
		this.end_page_no = end_page_no;
	}

	public String getPage_ranges() {
		return page_ranges;
	}

	public void setPage_ranges(String page_ranges) {
		this.page_ranges = page_ranges;
	}

	public int getNo_of_pages() {
		return no_of_pages;
	}

	public void setNo_of_pages(int no_of_pages) {
		this.no_of_pages = no_of_pages;
	}

	public String getDuplication_flag() {
		return duplication_flag;
	}

	public void setDuplication_flag(String duplication_flag) {
		this.duplication_flag = duplication_flag;
	}

	public String getDup_of_article_AUID() {
		return dup_of_article_AUID;
	}

	public void setDup_of_article_AUID(String dup_of_article_AUID) {
		this.dup_of_article_AUID = dup_of_article_AUID;
	}

	public String getArticle_creator() {
		return article_creator;
	}

	public void setArticle_creator(String article_creator) {
		this.article_creator = article_creator;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public List<String> getIssn() {
		return issn;
	}

	public void setIssn(List<String> issn) {
		this.issn = issn;
	}

	public String getIsbn() {
		return isbn;
	}

	public void setIsbn(String isbn) {
		this.isbn = isbn;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public String getPii() {
		return pii;
	}

	public void setPii(String pii) {
		this.pii = pii;
	}

	public String getOclc() {
		return oclc;
	}

	public void setOclc(String oclc) {
		this.oclc = oclc;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getRights() {
		return rights;
	}

	public void setRights(String rights) {
		this.rights = rights;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}

	public String getProvider() {
		return provider;
	}

	public void setProvider(String provider) {
		this.provider = provider;
	}

	public DWIssue getParent_issue() {
		return parent_issue;
	}

	public void setParent_issue(DWIssue parent_issue) {
		this.parent_issue = parent_issue;
	}

	public String getJournal_ark_id() {
		return journal_ark_id;
	}

	public void setJournal_ark_id(String journal_ark_id) {
		this.journal_ark_id = journal_ark_id;
	}

	public String getIssue_ark_id() {
		return issue_ark_id;
	}

	public void setIssue_ark_id(String issue_ark_id) {
		this.issue_ark_id = issue_ark_id;
	}

	

}

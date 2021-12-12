package org.portico.tdm.tdm2.datawarehouse;


import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.tools.TDMUtil;


public class DWPage {
	
	//members from Analytics DB
	String arkId;
	String page_no;						//i, 1, issue cover
	
	String issn_no;						//also content set name
	String vol_no;
	String issue_no;
	String au_id;
	
	int seq_in_article;
	int seq_in_issue;
	
	String source_file;
	String page_display;				// article title page xx
	
	Timestamp creation_ts;
	Timestamp modification_ts;
	
	static Logger logger = LogManager.getLogger(DWPage.class.getName());
	static String programName = "DWPage";
	
	public DWPage(String au_id, int seq_in_article) {
		this.au_id = au_id;
		this.seq_in_article = seq_in_article;
	}
	
	
	public void insertPageToTable() {
		
		String au_id = getAu_id();
		String ark_id = getArkId();
		String issn_no = getIssn_no();
		String vol_no = getVol_no();
		String issue_no = getIssue_no();
		int seq_in_article = getSeq_in_article();
		int seq_in_issue = getSeq_in_issue();
		String source_file = getSource_file();
		String page_display = getPage_display();
		String page_no = getPage_no();
		
		String query = "insert into tdm_page (ark_id, page_no, issn_no, vol_no, issue_no, au_id, seq_in_article, seq_in_issue, source_file, page_display, creation_ts )"
						+ " values ('" + ark_id + "', '" + page_no + "', '" + issn_no + "', q'$" + vol_no + "$', q'$" + issue_no + "$', '" + au_id + "', "
						+ seq_in_article + ", " + seq_in_issue + ", '" + source_file + "', '" + page_display + "', current_timestamp )";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			stmt.executeQuery(query);
		}
		catch(Exception e) {
			logger.error( programName + ":insertPageToTable: " + query + " " + e.getMessage());
		}
			
	}
	
	
	
	
	
	
	public String getArkId() {
		return arkId;
	}
	public void setArkId(String arkId) {
		this.arkId = arkId;
	}
	public String getPage_no() {
		return page_no;
	}
	public void setPage_no(String page_no) {
		this.page_no = page_no;
	}
	public String getIssn_no() {
		return issn_no;
	}
	public void setIssn_no(String issn_no) {
		this.issn_no = issn_no;
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
	public int getSeq_in_article() {
		return seq_in_article;
	}
	public void setSeq_in_article(int seq_in_article) {
		this.seq_in_article = seq_in_article;
	}
	public int getSeq_in_issue() {
		return seq_in_issue;
	}
	public void setSeq_in_issue(int seq_in_issue) {
		this.seq_in_issue = seq_in_issue;
	}
	public String getSource_file() {
		return source_file;
	}
	public void setSource_file(String source_file) {
		this.source_file = source_file;
	}
	public String getPage_display() {
		return page_display;
	}
	public void setPage_display(String page_display) {
		this.page_display = page_display;
	}
	public Timestamp getCreation_ts() {
		return creation_ts;
	}
	public void setCreation_ts(Timestamp creation_ts) {
		this.creation_ts = creation_ts;
	}
	public Timestamp getModification_ts() {
		return modification_ts;
	}
	public void setModification_ts(Timestamp modification_ts) {
		this.modification_ts = modification_ts;
	}
	
	
	
	

}

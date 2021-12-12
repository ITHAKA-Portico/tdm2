package org.portico.tdm.tdm2.datawarehouse;


import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.tools.TDMUtil;

public class DWVolume {
	
	String content_set_name;
	String arkId;					//Not in use in V2
	String volumeNumber;	
	DWJournal parentJournal;
	int issueCount;
	List<DWIssue> issues;
	
	String pub_year;
	boolean issue_in_seq;
	int first_issueno_in_volume;
	String volume_completeness;
	String issue_range;
	
	static Logger logger = LogManager.getLogger(DWVolume.class.getName());
	static String programName = "DWVolume";
	
	public DWVolume() {
		issues = new ArrayList<>();
	}
	
	public DWVolume(String cs, String volumeNo) {
		this.content_set_name = cs;
		this.volumeNumber = volumeNo;
		
		issues = new ArrayList<>();
	}
	
	
	public void populatePorticoVolumeMetadata( ) throws Exception {
		String csname = getContent_set_name();
		String volNo = getVolumeNumber();
		
		String query = "select * from dw_volume where issn_no='" + csname + "' and vol_no=q'$" + volNo + "$'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query);
			
			if ( rs.next()) {

				String pub_year = rs.getString("pub_year");
				String issue_in_seq_char = rs.getString("issue_in_seq");
				int first_issue_no = rs.getInt("first_issueno_in_volume");
				String issue_range = rs.getString("issue_range");
				int archived_issue_count = rs.getInt("archived_issue_count");
				String volume_completeness = rs.getString("volume_completeness");
				String journal_title = rs.getString("journal_title");
				String ark_id = rs.getString("ark_id");
				
				if ( ark_id == null ) {
					 ark_id = createArkIdForVolume( );
				}
				
				setArkId(ark_id);
				
				setPub_year(pub_year);
				setIssue_range(issue_range);
				if ( issue_in_seq_char != null && issue_in_seq_char.equalsIgnoreCase("y")) {
					setIssue_in_seq(true);
				}
				else if ( issue_in_seq_char != null && issue_in_seq_char.equalsIgnoreCase("n")) {
					setIssue_in_seq(false);
				}
				setFirst_issueno_in_volume(first_issue_no);
				setIssueCount(archived_issue_count);
				setVolume_completeness(volume_completeness);
				

			}
			else{
				logger.info( programName + ":populatePorticoVolumeMetadata: Empty archived volumes");
			}

			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * Populate PublicationVolume.issues from dw_issue table. Create ARK id for DWIssue if it is null
	 * @param conn
	 * @throws Exception
	 */
	public void findAllArchivedIssuesInVolume() throws Exception {
		
		String csname = getContent_set_name();
		String volNo = getVolumeNumber();
		issues = new ArrayList<>();

		String query = "select * from dw_issue where issn_no='" + csname + "' and vol_no=q'$" + volNo + "$' and issue_type='Physical' order by all_to_number(issue_no)";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			while( rs.next() ) {

				String issue_no = rs.getString("issue_no");
				String journal_title = rs.getString("journal_title");
				String pub_year = rs.getString("pub_year");
				int seq_in_volume = rs.getInt("seq_in_volume");
				int article_count = rs.getInt("article_count");
				int ic_article_count = rs.getInt("ic_article_count");
				Date pub_date = rs.getDate("pub_date");
				String issue_completeness = rs.getString("issue_completeness");
				String binding = rs.getString("binding");
				String ark_id = rs.getString("ark_id");
				
				DWIssue issue = new DWIssue(csname, volNo, issue_no);
				issue.setIssue_no(issue_no);
				issue.setParent_volume(this);
				issue.setJournal_title(journal_title);
				issue.setPub_year(pub_year);
				issue.setPub_date(pub_date);
				issue.setIssue_completeness(issue_completeness);
				issue.setBinding(binding);
				issue.setArticle_count(article_count);
				issue.setSeq_in_volume(seq_in_volume);
				issue.setWebsite_article_count(ic_article_count);
				if ( ark_id == null ) {
				    ark_id = issue.createArkIdForIssue();
				}
				issue.setArkId(ark_id);

				issues.add(issue);
				
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		
		
	}
	
	
	
	
	public String createArkIdForVolume() throws Exception {
		String csname = getContent_set_name();
		String volno = getVolumeNumber();
		String ark_id = null;
		
		try {
			ark_id = TDMUtil.getNoid( "DEV" );
			
			if ( !ark_id.startsWith("ark:")) {
				ark_id = "ark://" + ark_id;
			}
		} catch (Exception e) {
			throw e;
		}
		
		String query = "update dw_volume set ark_id='" + ark_id + "' where issn_no='" + csname + "' and vol_no=q'$" + volno + "$'";

		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			stmt.executeUpdate(query);
		
		}
		catch(Exception e) {
			throw e;
		}
		
		return ark_id;
		
	}

	public String getContent_set_name() {
		return content_set_name;
	}

	public void setContent_set_name(String content_set_name) {
		this.content_set_name = content_set_name;
	}

	public String getArkId() {
		return arkId;
	}

	public void setArkId(String arkId) {
		this.arkId = arkId;
	}

	public String getVolumeNumber() {
		return volumeNumber;
	}

	public void setVolumeNumber(String volumeNumber) {
		this.volumeNumber = volumeNumber;
	}

	public DWJournal getParentJournal() {
		return parentJournal;
	}

	public void setParentJournal(DWJournal parentJournal) {
		this.parentJournal = parentJournal;
	}

	public int getIssueCount() {
		return issueCount;
	}

	public void setIssueCount(int issueCount) {
		this.issueCount = issueCount;
	}

	public List<DWIssue> getIssues() {
		return issues;
	}

	public void setIssues(List<DWIssue> issues) {
		this.issues = issues;
	}

	public String getPub_year() {
		return pub_year;
	}

	public void setPub_year(String pub_year) {
		this.pub_year = pub_year;
	}

	public boolean isIssue_in_seq() {
		return issue_in_seq;
	}

	public void setIssue_in_seq(boolean issue_in_seq) {
		this.issue_in_seq = issue_in_seq;
	}

	public int getFirst_issueno_in_volume() {
		return first_issueno_in_volume;
	}

	public void setFirst_issueno_in_volume(int first_issueno_in_volume) {
		this.first_issueno_in_volume = first_issueno_in_volume;
	}

	public String getVolume_completeness() {
		return volume_completeness;
	}

	public void setVolume_completeness(String volume_completeness) {
		this.volume_completeness = volume_completeness;
	}

	public String getIssue_range() {
		return issue_range;
	}

	public void setIssue_range(String issue_range) {
		this.issue_range = issue_range;
	}
	


}

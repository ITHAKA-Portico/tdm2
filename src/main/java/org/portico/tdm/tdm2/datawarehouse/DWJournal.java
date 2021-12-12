package org.portico.tdm.tdm2.datawarehouse;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.tools.TDMUtil;

public class DWJournal {

	String content_set_name;			//also content set name

	String arkId;						
	String journal_title;
	Set<String> journal_issn;			//issns from cmi_agreement_cs_identifiers
	String publisher;
	String publisherID;	
	List<Identifier> identifiers;
	List<String> issns;					//XXXX-XXXX format
	
	String hasOA;
	List<DWVolume> volumes;
	String copyrightHolder;

	//other members
	static Logger logger = LogManager.getLogger(DWJournal.class.getName());
	static String programName = "DWJournal";
	
	public DWJournal(String csname ) {
		this.content_set_name = csname;
		
		volumes = new ArrayList<>();
		issns = new ArrayList<>();
		identifiers = new ArrayList<>();
	}
	
	public void populateDWJournal( ) {
		
		String query1 = "select title_name, provider_name, provider_id, copyright_holder, has_oa from cmi_agreement_content_sets where content_set_name='" 
							+ getContent_set_name() + "'";
		String query2 = "select  identifier_value from cmi_agreement_cs_identifiers where content_set_name='" + getContent_set_name() + "' and identifier_type='issn'";
		

		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			
			if ( rs.next()) {
				String provider = rs.getString("provider_name");
				String provider_id = rs.getString("provider_id");
				String journal_title = rs.getString("title_name");
				String copyright_holder = rs.getString("copyright_holder");
				String has_oa = rs.getString("has_oa");
				
				setPublisher(provider);
				setPublisherID(provider_id);
				setJournal_title(journal_title);
				setCopyrightHolder(copyright_holder);
				setHasOA(has_oa);
			}
			else {
				logger.error( programName + ":populateJournal: No info for content set " + content_set_name );
			}
			
			rs = stmt.executeQuery(query2);
			journal_issn = new HashSet<String>();

			while( rs.next() ) {
				String issn = rs.getString("identifier_value");
				journal_issn.add(issn);
			}
			
			rs.close();
			
			for(String issn_str: journal_issn) {
				try {
					issns.add(TDMUtil.formatISSN(issn_str)); //Format value to XXXX-XXXX format.
				}
				catch(Exception e) {
					logger.error(e.getMessage());
				}
			}
			
		}
		catch(Exception e) {
			
		}
		
		try {
			createArkIdForJournal();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	/**
	 * Read ark_id from dw_journal table. If null, insert one.
	 * @throws Exception
	 */
	private void createArkIdForJournal() throws Exception {
		String csname = getContent_set_name();
		String query1 = "select ark_id from dw_journal where issn_no='" + csname + "'";
		
		String ark_id = null;
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			
			if ( rs.next()) {
				ark_id = rs.getString("ark_id");
		
			}
			
			rs.close();
			
			if ( ark_id == null ) {
				try {
					ark_id = TDMUtil.getNoid( "DEV" );
					
					if ( !ark_id.startsWith("ark:")) {
						ark_id = "ark://" + ark_id;
					}
				} catch (Exception e) {
					throw e;
				}
				
				String query2 = "update dw_journal set ark_id='" + ark_id + "' where issn_no='" + csname + "'";
				stmt.executeUpdate(query2);
			}
			
			setArkId(ark_id);
		
		}
		catch(Exception e) {
			throw e;
		}
		
		
	}
	
	
	public void findAllArchivedVolumes() throws Exception {
		
		String csname = getContent_set_name();

		String query = "select * from dw_volume where issn_no='" + csname + "' and volume_type='Physical' order by all_to_number(vol_no)";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query);
			
			while ( rs.next()) {
				String vol_no = rs.getString("vol_no");
				String pub_year = rs.getString("pub_year");
				String issue_in_seq_char = rs.getString("issue_in_seq");
				int first_issue_no = rs.getInt("first_issueno_in_volume");
				String issue_range = rs.getString("issue_range");
				int archived_issue_count = rs.getInt("archived_issue_count");
				String volume_completeness = rs.getString("volume_completeness");
				String ark_id = rs.getString("ark_id");

				DWVolume volume = new DWVolume(csname, vol_no);
				volume.setPub_year(pub_year);
				volume.setIssue_range(issue_range);
				if ( issue_in_seq_char != null && issue_in_seq_char.equalsIgnoreCase("y")) {
					volume.setIssue_in_seq(true);
				}
				else if ( issue_in_seq_char != null && issue_in_seq_char.equalsIgnoreCase("n")) {
					volume.setIssue_in_seq(false);
				}
				volume.setFirst_issueno_in_volume(first_issue_no);
				volume.setIssueCount(archived_issue_count);
				volume.setVolume_completeness(volume_completeness);
				volume.setParentJournal(this);
				if ( ark_id == null ) {
					ark_id = volume.createArkIdForVolume();
				}
				volume.setArkId(ark_id);
				
				volumes.add(volume);
			}

			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}

	}
	


	public String getContent_set_name() {
		return content_set_name;
	}

	public void setContent_set_name(String content_set_name) {
		this.content_set_name = content_set_name;
	}

	public String getJournal_title() {
		return journal_title;
	}

	public void setJournal_title(String journal_title) {
		this.journal_title = journal_title;
	}

	public Set<String> getJournal_issn() {
		return journal_issn;
	}

	public void setJournal_issn(Set<String> journal_issn) {
		this.journal_issn = journal_issn;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String provider) {
		this.publisher = provider;
	}

	public List<Identifier> getIdentifiers() {
		return identifiers;
	}

	public void setIdentifiers(List<Identifier> identifiers) {
		this.identifiers = identifiers;
	}

	public List<String> getIssns() {
		return issns;
	}

	public void setIssns(List<String> issns) {
		this.issns = issns;
	}

	public String getHasOA() {
		return hasOA;
	}

	public void setHasOA(String hasOA) {
		this.hasOA = hasOA;
	}

	public List<DWVolume> getVolumes() {
		return volumes;
	}

	public void setVolumes(List<DWVolume> volumes) {
		this.volumes = volumes;
	}

	public String getCopyrightHolder() {
		return copyrightHolder;
	}

	public void setCopyrightHolder(String copyrightHolder) {
		this.copyrightHolder = copyrightHolder;
	}

	public String getPublisherID() {
		return publisherID;
	}

	public void setPublisherID(String publisherID) {
		this.publisherID = publisherID;
	}

	public String getArkId() {
		return arkId;
	}

	public void setArkId(String arkId) {
		this.arkId = arkId;
	}


	
}

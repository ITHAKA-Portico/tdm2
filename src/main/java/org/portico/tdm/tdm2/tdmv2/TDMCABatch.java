package org.portico.tdm.tdm2.tdmv2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.portico.tdm.tdm2.tools.TDMUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TDMCABatch {
	
	static Logger logger = LogManager.getLogger(TDMCABatch.class.getName());
	static String programName = "TDMCABatch";
	
	String collectionId;
	String url ;
	String name ;
	String ingested ;
	String awardee_url ;
	String awardee_name ;
	int issue_processed;
	
	List<String> lccns;

	public TDMCABatch(JSONObject batch_json) {
		
		this.collectionId = "Chronicling America";
		
   		this.url = (String) batch_json.get("url");
   		this.name = (String) batch_json.get("name");
   		this.ingested = (String) batch_json.get("ingested");
   		this.ingested = ingested.substring(0, 10);
   		this.awardee_url = null;
   		this.awardee_name = null;
		
		JSONObject awardee_obj = (JSONObject) batch_json.get("awardee"); 
		this.awardee_url = (String) awardee_obj.get("url");
		this.awardee_name = (String) awardee_obj.get("name");
		  		
		JSONArray lccns_array = ((JSONArray)batch_json.get("lccns")); 
		lccns = new ArrayList<>();
 		Iterator itr3 = lccns_array.iterator(); 

		while (itr3.hasNext()) { 
			String lccn = (String) itr3.next();
			lccns.add(lccn);
		}
	}
	
	public void checkLccns() {
		
		String name = getName();
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement statement1 = conn.createStatement(); 
				Statement statement2 = conn.createStatement();		) {


			for(String lccn: getLccns()) {
				
				String query1 = "select count(*) from tdm_batch where batch_name='" + name + "' and lccn='" + lccn + "'";
				String query2 = "select count(*) from tdm_newspaper where lccn='" + lccn + "'";
				int issue_count = 0;
				
				try {
					ResultSet rs = statement1.executeQuery(query2);
					
					if ( rs.next()) {
						issue_count = rs.getInt(1);
						
						rs = statement1.executeQuery(query1);
						if ( rs.next()) {
							int count = rs.getInt(1);

							if ( count == 0 ) {		//this is a new lccn
								insertALccnToTable( lccn, issue_count );

							}
							else {
							
								if ( count != 0 ) {		//this lccn has been processed before
									updateTable(lccn, issue_count);
								}
							}
						}


						rs.close();
					}
				}
				catch(Exception e) {
					logger.error( programName + ":checkLccns :Error iupdate tdm_batch table " + name + ", " + lccn + ", " + awardee_url );
				}

			}
		}
		catch(Exception e) {
			logger.error( programName + ":checkLccns :Error iupdate tdm_batch table " + name  + awardee_url );
		}
		
	}
	
	private void updateTable(String lccn, int issue_count) {
		
		String collectionId = getCollectionId();
		String url = getUrl();
		String name = getName();
		String ingested = getIngested() ;
		String awardee_url = getAwardee_url();
		String awardee_name = getAwardee_name();
		
		String updateQuery = "update tdm_batch set collection_id=?, batch_url=?, issue_count_processed=?, awardee_url=?, awardee_name=?, ingest_time=? "
							+ "where batch_name=? and lccn=?";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				PreparedStatement statement1 = conn.prepareStatement(updateQuery); ) {
			
			statement1.setString(1, collectionId);
			statement1.setString(2, url);
			statement1.setInt(3,  issue_count);
			statement1.setString(4, awardee_url);
			statement1.setString(5, awardee_name);
			statement1.setString(6, ingested);
			statement1.setString(7, name);
			statement1.setString(8, lccn);
			
			statement1.executeUpdate();
			
		}
		catch(Exception e) {
			logger.error( programName + ":updateTable :Error iupdate tdm_batch table " + name + ", " + lccn + ", " + awardee_url );
		}
		
		
		
	}

	/**
	 * Insert a new row of lccn into tdm_batch table.
	 * @param lccn
	 * @param issue_count 
	 */
	private void insertALccnToTable(String lccn, int issue_count) {
		
		String collectionId = getCollectionId();
		String url = getUrl();
		String name = getName();
		String ingested = getIngested().substring(0, 10) ;
		String awardee_url = getAwardee_url();
		String awardee_name = getAwardee_name();
		
		String insertQuery = "insert into tdm_batch (collection_id, batch_name, batch_url, lccn, issue_count_processed, awardee_url, awardee_name, ingest_time, creation_ts) "
							+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				PreparedStatement statement1 = conn.prepareStatement(insertQuery); ) {
			
			statement1.setString(1, collectionId);
			statement1.setString(2, name);
			statement1.setString(3, url);
			statement1.setString(4, lccn);
			statement1.setInt(5,  issue_count);
			statement1.setString(6, awardee_url);
			statement1.setString(7, awardee_name);
			statement1.setString(8, ingested);
			Timestamp sqlDate = new java.sql.Timestamp( new java.util.Date().getTime());
			statement1.setTimestamp(9, sqlDate);
			
			statement1.executeUpdate();
			
		}
		catch(Exception e) {
			logger.error( programName + ":insertALccnToTable :Error insert lccn to tdm_batch table " + name + ", " + lccn + ", " + awardee_url );
		}
		
		
	}

	public String getCollectionId() {
		return collectionId;
	}

	public void setCollectionId(String collectionId) {
		this.collectionId = collectionId;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIngested() {
		return ingested;
	}

	public void setIngested(String ingested) {
		this.ingested = ingested;
	}

	public String getAwardee_url() {
		return awardee_url;
	}

	public void setAwardee_url(String awardee_url) {
		this.awardee_url = awardee_url;
	}

	public String getAwardee_name() {
		return awardee_name;
	}

	public void setAwardee_name(String awardee_name) {
		this.awardee_name = awardee_name;
	}

	public List<String> getLccns() {
		return lccns;
	}

	public void setLccns(List<String> lccns) {
		this.lccns = lccns;
	}

	public int getIssue_processed() {
		return issue_processed;
	}

	public void setIssue_processed(int issue_processed) {
		this.issue_processed = issue_processed;
	}



}

package org.portico.tdm.tdm2.datawarehouse;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.tools.TDMUtil;

public class DWIssue {
	
	//members from Analytics DB
	String content_set_name;						//issn_no
	String vol_no;
	String issue_no;
	String journal_title;
	String pub_year;
	Date pub_date;
	Timestamp timestamp;
	String issue_type;
	String issue_completeness;
	String binding;
	String doi;
	
	int article_count;
	int website_article_count;
	int seq_in_volume;
	
	DWJournal parent_journal;
	DWVolume parent_volume;
	
	List<DWArticle> articles;
	String arkId;				
	
	//other members
	static Logger logger = LogManager.getLogger(DWIssue.class.getName());
	static String programName = "DWIssue";

	public DWIssue(String csname, String volno, String issueno ) {
		this.content_set_name = csname;
		this.vol_no = volno;
		this.issue_no = issueno;
		
		articles = new ArrayList<>();
	}
	

	
	/**
	 * Populate DWIssue member values from dw_issue table
	 * @throws Exception
	 */
	public void populateDWIssue() throws Exception {
		
		String csname = getContent_set_name();
		String volNo = getVol_no();
		String issueNo = getIssue_no();
		
		String query = "select * from dw_issue where issn_no='" + csname + "' and vol_no=q'$" + volNo + "$' and issue_no=q'$" + issueNo + "$'";
		
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
				
				if ( ark_id == null ) {
					ark_id = createArkIdForIssue();
				}

				setJournal_title(journal_title);
				setPub_year(pub_year);
				setPub_date(pub_date);
				setIssue_completeness(issue_completeness);
				setBinding(binding);
				setArticle_count(article_count);
				setSeq_in_volume(seq_in_volume);
				setWebsite_article_count(ic_article_count);
				setArkId( ark_id);

			
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
	}

	
	public String createArkIdForIssue() throws Exception {
		String csname = getContent_set_name();
		String volno = getVol_no();
		String issueno = getIssue_no();
		
		String ark_id = null;
		
		try {
			ark_id = TDMUtil.getNoid( "DEV" );
			
			if ( !ark_id.startsWith("ark:")) {
				ark_id = "ark://" + ark_id;
			}
		} catch (Exception e) {
			throw e;
		}
		
		String query = "update dw_issue set ark_id='" + ark_id + "' where issn_no='" + csname + "' and vol_no=q'$" + volno + "$' "
						+ " and issue_no=q'$" + issueno + "$'";

		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			stmt.executeUpdate(query);
		
		}
		catch(Exception e) {
			throw e;
		}
		
		return ark_id;
		
	}
	
	/**
	 * Find all de-duplicated articles in an issue and populate their metadata, trying to be in correct order( article_seq, start_page_no, display_label)
	 * @param conn
	 * @throws Exception 
	 */
	public void findAndPopulateMetadataForAllArticlesInIssue() throws Exception {
		
		String csname = getContent_set_name();
		String volNo = getVol_no();
		String issueNo = getIssue_no();
		String issue_id = csname + " v." + volNo + " n." + issueNo;
		
		List<DWArticle> deduped_articles = new ArrayList<>();
		
		//article query
		String query1 = "select * from dw_article, a_au_dmd where issn_no='" + csname + "' and vol_no=q'$" + volNo + "$' and issue_no=q'$" 
		                            + issueNo + "$' and ( duplication_flag='N' or duplication_flag is null) and dw_article.au_id=a_au_dmd.a_au_id order by article_seq, all_to_number(start_page_no), display_label";
		String xml_query = "select pmd_object_id from a_su where a_au_id=? and pmd_status='Active' and pmd_mime_type='application/xml'";
		

		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();
				PreparedStatement prst = conn.prepareStatement(xml_query); ) {
			
			ResultSet rs = stmt.executeQuery(query1);

			int articleIndex = 1;
			while( rs.next() ) {
				String au_id = rs.getString("au_id");
				//System.out.println(au_id);
				String journal_title = rs.getString("journal_title");
				String article_title = rs.getString("article_title");
				String article_creator = rs.getString("article_creator");
				String start_page_no = rs.getString("start_page_no");
				String end_page_no = rs.getString("end_page_no");
				String page_range = rs.getString("page_range");
				String pub_year = rs.getString("pub_year");
				int no_of_pages = rs.getInt("no_of_pages");
				Date pub_date = rs.getDate("pub_date");
				String sort_key = rs.getString("sort_key");
				String display_label = rs.getString("display_label");
				Timestamp timestamp = rs.getTimestamp("timestamp");
				String dup_of_article_AUID = rs.getString("dup_of_article_auid");
				
				String pmd_language = rs.getString("pmd_language");
				
				DWArticle article = new DWArticle(au_id);
				article.setJournal_title(journal_title);
				article.setContent_set_name(csname);
				article.setVol_no(volNo);
				article.setIssue_no(issueNo);
				article.setStart_page_no(start_page_no);
				article.setEnd_page_no(end_page_no);
				if ( page_range != null && page_range.indexOf("-") == -1 && page_range.indexOf(",") == -1 ) {
					if ( start_page_no != null && end_page_no != null && start_page_no.equals(end_page_no)) {
						page_range = "pp. " + start_page_no;
					}
					else {
						page_range = "pp. " + start_page_no + "-" + end_page_no;
					}
				}
				
				article.setPage_ranges(page_range);
				article.setArticle_title(article_title);
				//List<Person> article_creators = TDMUtil.convertPMDCreatorsToPersonList( article_creator, au_id );
				article.setArticle_creator(article_creator);
				article.setLanguage(pmd_language);
				article.setPub_date(pub_date);
				article.setPub_year(pub_year);

				article.setArticle_seq(articleIndex++);
				article.setNo_of_pages(no_of_pages);
				article.setSort_key(sort_key);
				article.setDisplay_label(display_label);
				article.setTimestamp(timestamp);
				article.setDup_of_article_AUID(dup_of_article_AUID);
				
				try {
					article.populateArticleIdentifiers();
				}
				catch(Exception e) {
					logger.error( programName + ":findAndPopulateMetadataForAllArticlesInIssue:populateArticleIdentifiers: " + au_id + " " + e.getMessage()  );
				}
				

				article.setParent_issue(this);
		
				deduped_articles.add(article);
				
				
			}
			
			rs.close();
			
			setArticles(deduped_articles);
		}
		catch(Exception e) {
			logger.error( programName + ":findAndPopulateMetadataForAllArticlesInIssue: " + issue_id + " " + e.getMessage()  );
			throw e;
		}

	}
	
		

	public List<String> findAllArticleAUsInIssue() throws Exception {
		
		String csname = getContent_set_name();
		String volNo = getVol_no();
		String issueNo = getIssue_no();
		String issue_id = csname + " v." + volNo + " n." + issueNo;

		List<String> au_list = new ArrayList<>();
		
		String query1 = "select * from dw_article where issn_no='" + csname + "' and vol_no=q'$" + volNo + "$' and issue_no=q'$" 
                + issueNo + "$' and ( duplication_flag='N' or duplication_flag is null) order by article_seq, all_to_number(start_page_no), display_label";


		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement(); ) {

			ResultSet rs = stmt.executeQuery(query1);

			while( rs.next() ) {
				String au_id = rs.getString("au_id");
				au_list.add(au_id);
			}
		}
		catch(Exception e) {
			logger.error( programName + ":findAllArticleAUsInIssue: " + issue_id + " " + e.getMessage()  );
			throw e;
		}


		return au_list;
		
		
	}
	

	public void updateDateInDB(Connection conn) throws Exception {
		
		String pub_year = getPub_year();
		Date pub_date = getPub_date();
		String csname = getContent_set_name();
		String volNo = getVol_no();
		String issueNo = getIssue_no();

		
		String query = "update dw_issue set pub_year=?, pub_date=? where issn_no=? and vol_no=? and issue_no=? ";
		try ( PreparedStatement stmt = conn.prepareStatement(query); ) {
			stmt.setString(1, pub_year);
			stmt.setDate(2, new java.sql.Date(pub_date.getTime()));
			stmt.setString(3, csname);
			stmt.setString(4, volNo);
			stmt.setString(5, issueNo);
			
			stmt.executeQuery();
			
		}
		catch(Exception e) {
			logger.error( programName + ":updateDateInDB " + csname + " " + volNo + " " + issueNo + " " + e);
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

	public String getJournal_title() {
		return journal_title;
	}

	public void setJournal_title(String journal_title) {
		this.journal_title = journal_title;
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

	public String getIssue_type() {
		return issue_type;
	}

	public void setIssue_type(String issue_type) {
		this.issue_type = issue_type;
	}

	public String getIssue_completeness() {
		return issue_completeness;
	}

	public void setIssue_completeness(String issue_completeness) {
		this.issue_completeness = issue_completeness;
	}

	public String getBinding() {
		return binding;
	}

	public void setBinding(String binding) {
		this.binding = binding;
	}

	public int getArticle_count() {
		return article_count;
	}

	public void setArticle_count(int article_count) {
		this.article_count = article_count;
	}

	public int getWebsite_article_count() {
		return website_article_count;
	}

	public void setWebsite_article_count(int website_article_count) {
		this.website_article_count = website_article_count;
	}

	public int getSeq_in_volume() {
		return seq_in_volume;
	}







	public void setSeq_in_volume(int seq_in_volume) {
		this.seq_in_volume = seq_in_volume;
	}







	public DWJournal getParent_journal() {
		return parent_journal;
	}

	public void setParent_journal(DWJournal parent_journal) {
		this.parent_journal = parent_journal;
	}







	public DWVolume getParent_volume() {
		return parent_volume;
	}







	public void setParent_volume(DWVolume parent_volume) {
		this.parent_volume = parent_volume;
	}







	public List<DWArticle> getArticles() {
		return articles;
	}







	public void setArticles(List<DWArticle> articles) {
		this.articles = articles;
	}



	public String getArkId() {
		return arkId;
	}



	public void setArkId(String arkId) {
		this.arkId = arkId;
	}



	public String getDoi() {
		return doi;
	}



	public void setDoi(String doi) {
		this.doi = doi;
	}



}

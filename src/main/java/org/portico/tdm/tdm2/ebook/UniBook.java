package org.portico.tdm.tdm2.ebook;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.datawarehouse.Identifier;
import org.portico.tdm.tdm2.tools.TDMUtil;

/**
 * This class is used to present ANA_UNIFIED_BOOK object for TDM purpose.
 * The main part of this class is copied from org.portico.ebook.Ebook class. 
 * It only has query functions, not insert or update functions.
 * 
 * @author dxie
 *
 */
public class UniBook {

	public final String programName = "UniBook";
	static Logger logger = LogManager.getLogger(UniBook.class.getName());
	
	int unibookID;
	String title;
	String subTitle;
	String creator;
	boolean series;		//true if this book itself is a series
	int seriesID = -1;
	String seriesTitle;
	String seriesVolNo;
	String seriesISSN;
	String contentSetName;
	String sortTitle;
	String displayTitle;
	String pubYear;
	String edition;
	String language;
	String publisherTitleFileName;
	String createdBy;
	Timestamp creationTS;
	String contentOwner;
	String publisher;
	String provider_id;
	String provider_name;
	String has_pca;			//This field was added after "DARE" was manully inserted into ANA_UNIFIED_BOOK table. "Yes" or "No"
	//String copyrightYear;    removed. Combined to pubYear.
	Set<Identifier> identifiers;
	Set<String> holdings;


	TDMUtil util ;
	
	String server = "PROD";		//default
	
	/**
	 * This constructor allows to construct an Ebook object from publisher title file.
	 * @param booktitle
	 * @throws Exception
	 */
	public UniBook(String booktitle) throws Exception {
		
		if ( TDMUtil.isNullOrEmpty(booktitle)) {
			throw new Exception("Empty title for UniBook");
		}
		
		title = booktitle;
		unibookID = -1;
		identifiers = new HashSet<>();
		
		util = new TDMUtil();
	}
	
	/**
	 * This constructor allows to construct an Ebook object from DB tables.
	 * @param unibookid
	 */
	public UniBook(int unibookid) {
		setUnibookID(unibookid);
		identifiers = new HashSet<>();
		
		util = new TDMUtil();
	}
	


	/**
	 * Use populateEbookFromDB(Connection) for better performance
	 * This method allows to populate Ebook object from DB tables.
	 * @throws Exception 
	 */
	public void populateUniBookFromDBByUnibookId() throws Exception {
		
		if ( getUnibookID() == -1 ) {
			logger.error( programName + ": populateUniBookFromDBByUnibookId: unibook_ID has not been set.");
			throw new Exception("Empty unibook_ID");
		}
		
		//first query ana_unified_book and ana_publisher_title_file
		String query = "select book.*, ana_publisher_title_file.file_name from ana_unified_book book, ana_publisher_title_file "
							+ "where unibook_id=" + getUnibookID() + " and book.title_file_id=ana_publisher_title_file.title_file_id";
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement() ) {

			ResultSet rs = stmt.executeQuery( query );
			
			if ( rs.next() ) {
				String title = rs.getString("title");
				String sub_title = rs.getString("sub_title");
				String creator = rs.getString("creator");
				String isSeries = rs.getString("is_series");
				String series_title = rs.getString("series_title");
				String series_volume = rs.getString("series_vol");
				int series_id = rs.getInt("series_id");
				String content_set_name = rs.getString("content_set_name");
				String sort_title = rs.getString("sort_title");
				String display_title = rs.getString("display_title");
				String pub_year = rs.getString("pub_year");
				String edition = rs.getString("edition");
				String language = rs.getString("language");
				String publisher = rs.getString("publisher");
				String title_file = rs.getString("file_name");
				String created_by = rs.getString("created_by");
				Timestamp ts = rs.getTimestamp("creation_ts");
				String content_owner = rs.getString("content_owner");
				String provider_id = rs.getString("provider_id");
				String provider_name = rs.getString("provider_name");
				//String copyright_year = rs.getString("copyright_year");
				String has_pca = rs.getString("has_pca");
				
				if ( series_id != 0 ) {
					String series_issn = findSeriesISSNs( conn, series_id );
					setSeriesISSN(series_issn);
				}
				setTitle(title);
				setSubTitle(sub_title);
				setCreator(creator);
				if ( "yes".equals(isSeries)) {
					setSeries(true);
				}
				else if ( "no".equals(isSeries)) {
					setSeries(false);
				}
				setSeriesTitle(series_title);
				setSeriesVolNo(series_volume);
				setContentSetName(content_set_name);
				setSortTitle(sort_title);
				setDisplayTitle(display_title);
				setPubYear(pub_year);
				setEdition(edition);
				setPublisher(publisher);
				setLanguage(language);
				setCreatedBy(created_by);
				setCreationTS(ts);
				setContentOwner(content_owner);
				setPublisherTitleFileName(title_file);
				setProvider_id(provider_id);
				setProvider_name( provider_name);
				//setCopyrightYear(copyright_year);
				setHasPca( has_pca);
				

			}
		}
		catch(SQLException sqle) {
			logger.error( programName + ": populateUniBookFromDBByUnibookId " + query + ". " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
			throw sqle;
		}
		catch(Exception e) {
			logger.error("Other Exception populateUniBookFromDBByUnibookId " + query + ". " + e.getMessage());
			throw e;
		}
		
		//query ana_ebook_identifier
		Set<Identifier> ids = new HashSet<>();
		query = "select * from ana_ebook_identifier where unibook_id=" + getUnibookID();
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement() ) {

			ResultSet rs = stmt.executeQuery( query );
			
			while ( rs.next() ) {
				String id_value = rs.getString("id_value");
				String id_type = rs.getString("id_type");
				String id_sub_type = rs.getString("id_sub_type");
				
				Identifier id = new Identifier( id_type, id_value);
				ids.add(id);
			}
			
			setIdentifiers(ids);
		}
		catch(SQLException sqle) {
			logger.error( programName + ": populateUniBookFromDBByUnibookId " + query + ". " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
			throw sqle;
		}
		catch(Exception e) {
			logger.error("Other Exception in populateUniBookFromDBByUnibookId " + query + ". " + e.getMessage());
			throw e;
		}
		
		
		//query ana_unibook_holding
		Set<String> holdingSet = new HashSet<>();
		query = "select * from ana_unibook_holding where unibook_id=" + getUnibookID();
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement() ) {

			ResultSet rs = stmt.executeQuery( query );
			
			while ( rs.next() ) {
				String holding_id = rs.getString("holding_id");
				
				holdingSet.add(holding_id);
				
			}
			
			if ( !holdingSet.isEmpty()) {
				setHoldings(holdingSet);
			}
			else {
				setHoldings( null );
			}
		}
		catch(SQLException sqle) {
			logger.error( programName + ": populateUniBookFromDBByUnibookId " + query + ". " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
			throw sqle;
		}
		catch(Exception e) {
			logger.error("Other Exception in populateUniBookFromDBByUnibookId " + query + ". " + e.getMessage());
			throw e;
		}
		

	}
	
	
	/**
	 * This method allows to populate UniBook object from DB ANA_ tables.
	 * @throws Exception 
	 */
	public void populateUniBookFromDB(Connection conn) throws Exception {
		
		if ( getUnibookID() == -1 ) {
			logger.error( programName + ": populateUniBookFromDB: unibook_ID has not been set.");
			throw new Exception("Empty unibook_ID");
		}
		
		//first query ana_unified_book and ana_publisher_title_file
		String query = "select book.*, ana_publisher_title_file.file_name from ana_unified_book book, ana_publisher_title_file "
							+ "where unibook_id=" + getUnibookID() + " and book.title_file_id=ana_publisher_title_file.title_file_id";
		try ( Statement stmt = conn.createStatement() ) {

			ResultSet rs = stmt.executeQuery( query );
			
			if ( rs.next() ) {
				String title = rs.getString("title");
				String sub_title = rs.getString("sub_title");
				String creator = rs.getString("creator");
				String isSeries = rs.getString("is_series");
				String series_title = rs.getString("series_title");
				String series_volume = rs.getString("series_vol");
				int series_id = rs.getInt("series_id");
				String content_set_name = rs.getString("content_set_name");
				String sort_title = rs.getString("sort_title");
				String display_title = rs.getString("display_title");
				String pub_year = rs.getString("pub_year");
				String edition = rs.getString("edition");
				String language = rs.getString("language");
				String publisher = rs.getString("publisher");
				String title_file = rs.getString("file_name");
				String created_by = rs.getString("created_by");
				Timestamp ts = rs.getTimestamp("creation_ts");
				String content_owner = rs.getString("content_owner");
				String provider_id = rs.getString("provider_id");
				String provider_name = rs.getString("provider_name");
				//String copyright_year = rs.getString("copyright_year");
				String has_pca = rs.getString("has_pca");
				
				if ( series_id != 0 ) {
					String series_issn = findSeriesISSNs( conn, series_id );
					setSeriesISSN(series_issn);
				}
				setTitle(title);
				setSubTitle(sub_title);
				setCreator(creator);
				if ( "yes".equals(isSeries)) {
					setSeries(true);
				}
				else if ( "no".equals(isSeries)) {
					setSeries(false);
				}
				setSeriesTitle(series_title);
				setSeriesVolNo(series_volume);
				setContentSetName(content_set_name);
				setSortTitle(sort_title);
				setDisplayTitle(display_title);
				setPubYear(pub_year);
				setEdition(edition);
				setPublisher(publisher);
				setLanguage(language);
				setCreatedBy(created_by);
				setCreationTS(ts);
				setContentOwner(content_owner);
				setPublisherTitleFileName(title_file);
				setProvider_id(provider_id);
				setProvider_name( provider_name);
				//setCopyrightYear(copyright_year);
				setHasPca( has_pca);
				

			}
			
			rs.close();
		}
		catch(SQLException sqle) {
			logger.error( programName + ": populateUniBookFromDB " + query + ". " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
			throw sqle;
		}
		catch(Exception e) {
			logger.error("Other Exception in populateUniBookFromDB " + query + ". " + e.getMessage());
			throw e;
		}
		
		//query ana_ebook_identifier
		Set<Identifier> ids = new HashSet<>();
		query = "select * from ana_ebook_identifier where unibook_id=" + getUnibookID();
		try ( Statement stmt = conn.createStatement() ) {

			ResultSet rs = stmt.executeQuery( query );
			
			while ( rs.next() ) {
				String id_value = rs.getString("id_value");
				String id_type = rs.getString("id_type");
				String id_sub_type = rs.getString("id_sub_type");
				
				Identifier id = new Identifier( id_type, id_value);
				ids.add(id);
			}
			
			rs.close();
			
			setIdentifiers(ids);
		}
		catch(SQLException sqle) {
			logger.error( programName + ": populateUniBookFromDB " + query + ". " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
			throw sqle;
		}
		catch(Exception e) {
			logger.error("Other Exception in populateUniBookFromDB " + query + ". " + e.getMessage());
			throw e;
		}
		
		
		//query ana_unibook_holding
		Set<String> holdingSet = new HashSet<>();
		query = "select * from ana_unibook_holding where unibook_id=" + getUnibookID();
		try ( Statement stmt = conn.createStatement() ) {

			ResultSet rs = stmt.executeQuery( query );
			
			while ( rs.next() ) {
				String holding_id = rs.getString("holding_id");
				
				holdingSet.add(holding_id);
				
			}
			
			rs.close();
			
			if ( !holdingSet.isEmpty()) {
				setHoldings(holdingSet);
			}
			else {
				setHoldings( null );
			}
		}
		catch(SQLException sqle) {
			logger.error(  programName + ": populateUniBookFromDB " + query + ". " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
			throw sqle;
		}
		catch(Exception e) {
			logger.error("Other Exception in populateUniBookFromDB " + query + ". " + e.getMessage());
			throw e;
		}
		

	}
	
	
	
	/**
	 * This method finds ISSN numbers for a series ebook.
	 * @param conn
	 * @param series_id
	 * @return
	 */
	private String findSeriesISSNs(Connection conn, int series_id) {
		String issn = null;
		String query = "select id_value from ana_ebook_identifier where unibook_id=" + series_id + " and id_type='issn'";
		
		try ( 	Statement stmt = conn.createStatement()) {

			ResultSet rs = stmt.executeQuery(query);
			
			while ( rs.next()) {
				if ( issn == null ) {
					issn = rs.getString("id_value");
				}
				else {
					issn += ";" + rs.getString("id_value");
				}
			}
    	}
    	catch(SQLException sqle) {
    		logger.error( programName + ": findSeriesISSN " + sqle.getMessage() );
			
    	}
		catch(Exception e) {
    		logger.error("Other Exception in findSeriesISSN " + e.getMessage());
     	}
		
		return issn;
	}

	/**
	 * This method adds an identifier to UniBook's identifier set
	 * @param identifier
	 */
	public void addIdentifier(Identifier identifier) {
		
		this.identifiers.add(identifier);
	}

	public String getHasPca() {
		return has_pca;
	}

	public void setHasPca(String pca) {
		this.has_pca = pca;
	}
	
	public void setUnibookID(int id) {
		this.unibookID = id;
	}


	public String getContentOwner() {
		return contentOwner;
	}

	public void setContentOwner(String contentOwner) {
		this.contentOwner = contentOwner;
	}
	
	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}


	
	
	public Set<Identifier> getISBNIdentifiers() {
		Set<Identifier> idset = new HashSet<>();
		
		for(Identifier id: identifiers) {
			if ( id.getName().equalsIgnoreCase("isbn") ) {
				idset.add(id);
			}
		}
		
		return idset;
	}
	
	
	public Set<Identifier> getISSNIdentifiers() {
		Set<Identifier> idset = new HashSet<>();
		
		for(Identifier id: identifiers) {
			if ( id.getName().equalsIgnoreCase("issn") ) {
				idset.add(id);
			}
		}
		
		return idset;
	}
	
	
	public Identifier getDOIdentifier() {
		Identifier doiid = null;
		
		for(Identifier id: identifiers) {
			if ( id.getName().equalsIgnoreCase("doi") ) {
				doiid = id;
				break;
			}
		}
		
		return doiid;
	}
	
	public Identifier getURLdentifier() {
		Identifier urlid = null;
		
		for(Identifier id: identifiers) {
			if ( id.getName().equalsIgnoreCase("url") ) {
				urlid = id;
				break;
			}
		}
		
		return urlid;
	}
	
	public Identifier getOCLCdentifier() {
		Identifier oclcid = null;
		
		for(Identifier id: identifiers) {
			if ( id.getName().equalsIgnoreCase("oclc") ) {
				oclcid = id;
				break;
			}
		}
		
		return oclcid;
	}
	


	/**
	 * This method returns unibook_id if the given identifier is found in ANA_EBOOK_IDENTIFIER table
	 * @param id
	 * @return
	 */
	private int findUnibookIDForIdentifier(Identifier id) {
		int unibook_id = -1;
		
		if ( id == null ) {
			logger.error(programName + ": findUnibookIDForIdentifier Null Identifier is used.");
			return unibook_id;
		}
		
		String query = "select unibook_id from ana_ebook_identifier where id_type='" + id.getName() + "' and id_value='" + id.getValue() + "'";
		
		try ( 	Connection conn = TDMUtil.getDWConnection_pooled("PROD");
    			Statement stmt = conn.createStatement()      	) {

			ResultSet rs = stmt.executeQuery( query );
			
			if ( rs.next() ) {
				unibook_id = rs.getInt(1);
			}
			
			rs.close();
    	}
    	catch(SQLException sqle) {
    		logger.error( programName + ": findUnibookIDForIdentifier Error in query " + query + ". " + sqle.getMessage() );
    		TDMUtil.printSQLException( sqle );
    	}
    	catch(Exception e) {
    		logger.error("Other Exception in findUnibookIDForIdentifier Error in query " + query + ". " + e.getMessage());
    	}

		return unibook_id;
	}

	


	/**
	 * Find unibook_id in ANA_UNIFIED_BOOK table, with given title, provider id and content set name.
	 * This method is used to find dup book when a book has no identifier.
	 * @param atitle
	 * @param acontentsetname
	 * @param supplying_provider_id 
	 * @return
	 */
	private Set<String> findUnibookIDs(Connection conn, String atitle, String acontentsetname, String supplying_provider_id) {
		Set<String> dupIds = new HashSet<>();
		
		
		if ( atitle == null || atitle.length() == 0) {
			logger.error(programName + ": findUnibookID Empty title is used.");
			return dupIds;
		}
		
		String query = "select ana_unified_book.unibook_id from ana_unified_book "
						+ " where lower(title)='" + TDMUtil.addSQLQuote(atitle).toLowerCase() + "' and content_set_name='" + acontentsetname + "' "
						+ " and provider_id='" + supplying_provider_id + "'";
		
		try ( 	Statement stmt = conn.createStatement()      	) {

			ResultSet rs = stmt.executeQuery( query );
			
			while ( rs.next() ) {
				int unibook_id = rs.getInt(1);
				dupIds.add( new Integer(unibook_id).toString());
			}
			
			rs.close();
    	}
    	catch(SQLException sqle) {
    		logger.error( programName + ": findUnibookID Error in query " + query + ". " + sqle.getMessage() );
    		TDMUtil.printSQLException( sqle );
    	}
    	catch(Exception e) {
    		logger.error("Other Exception in findUnibookID Error in query " + query + ". " + e.getMessage());
    	}

		return dupIds;
	}
	
	
	
	/**
	 * See if an ebook exists in ANA_UNIFIED_BOOK table, with same provider id and same identifier.
	 * @param id
	 * @param thistitle
	 * @param supplying_provider_id
	 * @return
	 */
	private Set<String> findUnibookIDs(Connection conn, Identifier id, String thistitle, String supplying_provider_id) {
		Set<String> dupIds = new HashSet<>();
		
		if ( id == null ) {
			logger.error(programName + ": findUnibookID Null Identifier is used.");
			return dupIds;
		}
		
		if ( thistitle == null || thistitle.length() == 0) {
			logger.error(programName + ": findUnibookID Empty title is used.");
			return dupIds;
		}
		
		String query = "select ana_unified_book.unibook_id, title from ana_ebook_identifier, ana_unified_book "
				+ " where ana_unified_book.unibook_id=ana_ebook_identifier.unibook_id "
				+ " and id_type='" + id.getName() + "' and id_value='" + id.getValue() + "' "
				+ " and provider_id='" + supplying_provider_id + "'";
		
		
		try ( 	Statement stmt = conn.createStatement()      	) {

			ResultSet rs = stmt.executeQuery( query );
			
			while ( rs.next() ) {
				String title = rs.getString("title");
				//if ( title.equalsIgnoreCase(thistitle) || TDMUtil.longestSubstring(title, thistitle).length() > 10 ) {
					int unibook_id = rs.getInt("unibook_id");
					dupIds.add( new Integer(unibook_id).toString());
				//}
			}
			
			rs.close();
    	}
    	catch(SQLException sqle) {
    		logger.error( programName + ": findUnibookID Error in query " + query + ". " + sqle.getMessage() );
    		TDMUtil.printSQLException( sqle );
    	}
    	catch(Exception e) {
    		logger.error("Other Exception in findUnibookID Error in query " + query + ". " + e.getMessage());
    	}

		return dupIds;
	}




	/**
	 * Finds UniBook from ANA_CMI_EBOOK_DETAIL table with same title and same identifier.
	 * @param id
	 * @param thistitle
	 * @return
	 */
	private String findANACMIUniBookID(Identifier id, String thistitle) {
		
		String ebook_id = null;
		
		if ( id == null ) {
			logger.error(programName + ": findANACMIUniBookID Null Identifier is used.");
			return ebook_id;
		}
		
		if ( thistitle == null || thistitle.length() == 0 ) {
			logger.error(programName + ": findANACMIEBookID Empty title is used.");
			return ebook_id;
		}
		
		String query = "select ana_cmi_ebook_identifier.ebook_id from ana_cmi_ebook_identifier, ana_cmi_ebook_detail "
							+ " where ana_cmi_ebook_detail.ebook_id=ana_cmi_ebook_identifier.ebook_id "
							+ " and title='" + TDMUtil.addSQLQuote(thistitle) + "' "
							+ " and identifier_value='" + id.getValue() + "'";  
		
		
		try ( 	Connection conn = TDMUtil.getDWConnection_pooled("PROD");
    			Statement stmt = conn.createStatement()      	) {

			ResultSet rs = stmt.executeQuery( query );
			
			if ( rs.next() ) {
				ebook_id = rs.getString("ebook_id");
				
			}
			
			rs.close();
    	}
    	catch(SQLException sqle) {
    		logger.error( programName + ": findANACMIEBookID Error in query " + query + ". " + sqle.getMessage() );
    	}
    	catch(Exception e) {
    		logger.error("Other Exception in findANACMIUniBookID Error in query " + query + ". " + e.getMessage());
    	}

		return ebook_id;
	}
	
	
	




	/**
	 * This method checks if a duplicate book exists in ANA_UNIFIED_BOOK tables. 
	 * Duplicate book means same title, same provider id, and at least one identifier is same.
	 * @param supplying_provider_id
	 * @return
	 */
	public boolean isANADupBook(Connection conn, String supplying_provider_id) {
		boolean result = false;
		
		Set<String> dupIds = findDuplicateEbooks( conn, supplying_provider_id, false );
		if ( dupIds != null && ! dupIds.isEmpty() ) {
			result = true;
		}
		
		return result;
	}
	
	


	/**
	 * This method checks if any of the book's identifiers exist in ANA_EBOOK_IDNETIFIER table
	 * @return
	 */
	public boolean checkIdentifiersDupStatus() {
		boolean result = false;
		
		for( Identifier id: getIdentifiers()) {
			int unibook_id = findUnibookIDForIdentifier(id);
			
			if ( unibook_id != -1 ) { 
				result = true;		//found one existing identifier
				break;
			}
		}
		
		return result;
	}



	/**
	 * THis method finds the unibook_id for the seires_title with same content_set_name
	 * @param series_title
	 * @return
	 */
	private int findSeriesID(String series_title) {
		int series_id = -1;
		
		String query = "select unibook_id from ana_unified_book where title='" + TDMUtil.addSQLQuote(series_title) + "' and content_set_name='" + getContentSetName() + "'";
		
		try ( 	Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement()) {

			ResultSet rs = stmt.executeQuery(query);
			
			if ( rs.next()) {
				series_id = rs.getInt("unibook_id");
			}
    	}
    	catch(SQLException sqle) {
    		logger.error( programName + ": findSeriesID " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
    	}
		catch(Exception e) {
    		logger.error("Other Exception in findSeriesID " + e.getMessage());
     	}
		
		return series_id;
	}



	

	/**
	 * This method queries CMI_PROVIDERS table
	 * @param provider_id
	 * @return provider_name
	 * @throws Exception 
	 * @throws SQLException 
	 */
	private String getProviderNameByID(String provider_id) throws SQLException, Exception {
		String provider_name = "";
		
		if ( provider_id.equalsIgnoreCase("BiblioRossica")) {		//provider_id=ASP, provider_name=BiblioRossica
			provider_name="BiblioRossica";
			return provider_name;
		}
		
		String query = "select provider_name from cmi_providers where provider_id='" + provider_id  + "'";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement() ) {
			
		
			ResultSet rs = stmt.executeQuery( query );
			
			if ( rs.next() ) {
				provider_name = rs.getString("provider_name");
			}
		}
		catch(SQLException sqle) {
			logger.error( programName + ": getProviderNameByID " + query + ". " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
		}
		catch(Exception e) {
			logger.error("Other Exception in getProviderNameByID " + query + ". " + e.getMessage());
		}


		return provider_name;
	}
	
	


	/**
	 * Finds all duplicate books's unibook_ids of this book from DB.
	 * A duplicate book means at least one of ISBN/DOI/OCLC identifiers is same, if identifiers exist, and titles are same or similar.
	 * @param conn 
	 * @param supplying_provider_id 
	 * @param titleonly 
	 * @return
	 */
	Set<String> findAllEbooksWithOverlappingIdentifiers(Connection conn, String supplying_provider_id, boolean titleonly) {
		Set<String> dupBooks = new HashSet<>();
		String thistitle = getTitle();
		String thiscontentsetname = getContentSetName();
		
		if ( getIdentifiers().isEmpty() || titleonly ) {		//some books has no identifier, will use title to find duplicates
			dupBooks = findUnibookIDs(conn, thistitle, thiscontentsetname, supplying_provider_id);
			return dupBooks;
		}
		
		for(Identifier id: getIdentifiers()) {
			String id_type = id.getName();

			if ( id_type.equalsIgnoreCase("isbn") || id_type.equalsIgnoreCase("doi") || id_type.equalsIgnoreCase("oclc") || id_type.equalsIgnoreCase("pii")
					//|| id_type.equals(Identifier.URL)  //one time for Elsevier 2018.9.17
					 ) {    //right now, do not use ISSN and URL to find duplicates
				Set<String> dupIds = findUnibookIDs(conn, id, thistitle, supplying_provider_id);
				
				if ( dupIds!= null && ! dupIds.isEmpty() ) {
					dupBooks.addAll( dupIds );
				}
			}
		}
		
		
		return dupBooks;
	}


	/**
	 * Check if input id matches one of current object's identifiers.
	 * @param idToCheck
	 * @return
	 */
	private boolean matchOneIdentifier(Identifier idToCheck) {
		boolean result = false;
		
		for(Identifier id: getIdentifiers())  {
			if ( id.equals(idToCheck)) {
				result = true;
				break;
			}
		}
		
		return result;
	}


	
	/**
	 * This method finds the title file name of the ebook object from DB
	 * @return
	 */
	public String getTitleFileName() {
		String file_name = null;
		
		String query = " select file_name from ana_unified_book, ana_publisher_title_file where unibook_id=" + getUnibookID()
				+ " and ana_unified_book.title_file_id=ana_publisher_title_file.title_file_id";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement() ) {
			ResultSet rs = stmt.executeQuery(query);
			
			if ( rs.next() ) {
				file_name = rs.getString("file_name");
			}
		}
		catch(Exception e ) {
			logger.error( programName + ": getFitleFileName " + query + " " + e.getMessage());
		}
		
		return file_name;
	}

	
	/**
	 * This method find all books in DB that are duplication of this book.
	 * Duplication books are: # from same provider, # have overlapping identifiers, # have same or similar title, # have same or similar author
	 * @param transaction_conn
	 * @param provider_id
	 * @param titleOnly
	 * @return
	 */
	public Set<String> findDuplicateEbooks(Connection transaction_conn,	String provider_id, boolean titleOnly ) {
		Set<String> dupBookIDs = new HashSet<>();
		String thisBookTitle = getTitle();
		String thisBookAuthor = getCreator();
		
		Set<String> booksToCheck = findAllEbooksWithOverlappingIdentifiers( transaction_conn, provider_id, false );  
		
		if ( booksToCheck == null || booksToCheck.isEmpty() ) {
			logger.info("No matching ebook for Publisher book\t\t\t" + thisBookTitle + "\t(" + thisBookAuthor +")");
			return dupBookIDs;
		}
		
		for(String candidBookID: booksToCheck ) {
			
			UniBook candidBook = new UniBook( Integer.parseInt(candidBookID) );
			candidBook.setServer( server );
			try {
				candidBook.populateUniBookFromDB( transaction_conn);
			} catch (Exception e) {
				logger.error( programName + ": findDuplicateUniBooks: populateUniBookFromDB for book " + candidBookID + ". " + e.getMessage());
				continue;		
			}
			
			if ( matchOtherBookOnDOIIdentifier( candidBook ) ) {	
				//no more other matches needed. 12/19/2017
				System.out.println("Publisher book\t\t\t" + thisBookTitle + "\t(" + thisBookAuthor + ")\n matched unified book(" + candidBookID + "):\ton DOI"   );
				dupBookIDs.add( candidBookID);
				continue;
			}

			//If other identifiers matched, need to compare title
			//compare title
			String candidBookTitle = candidBook.getTitle();
			
			if ( candidBookTitle == null || thisBookTitle == null ) { 
				continue;	//not a duplicate book
			}
			/* For JSTOR book project, temparory comment out title comparison */
			else if ( ! candidBookTitle.equalsIgnoreCase( thisBookTitle) &&  ! TDMUtil.almostSameTitle( candidBookTitle, thisBookTitle )) {
				logger.info( programName + ": findDuplicateEbooks Exclude book '" + candidBookTitle + "'(" + candidBookID + ") doesn't match title: '"+ thisBookTitle + "'(" + getUnibookID() +")");
				
				continue;
			}

			/*//compare author
			String candidBookAuthor = candidBook.getCreator();
			thisBookAuthor = TDMUtil.normalizeString(thisBookAuthor);
			candidBookAuthor = TDMUtil.normalizeString(candidBookAuthor);
			
			if ( candidBookAuthor != null && thisBookAuthor != null ) {
				if ( ! TDMUtil.almostSameAuthor(candidBookAuthor, thisBookAuthor)) {
					logger.info(PROGRAM_NAME + ":findDuplicateEbooks Exclude book " + candidBookAuthor + "(" + candidBookID + ") doesn't match author: " + thisBookAuthor + "(" + getUnibookID() + ")");
					continue;	//not a duplicate book
				}
			}*/
			
			//System.out.println("Matched to ANA ebooks:\t" + candidBookTitle + "\t(" + candidBookAuthor + ")");
			System.out.println("Publisher book\t\t\t" + thisBookTitle + "\t(" + thisBookAuthor + ")\n matched unified book(" + candidBookID + "):\t"+ candidBookTitle   );
			
			//for similar title and creator, candidBook is a duplicate of pubEbook
			dupBookIDs.add( candidBookID);
		}
		
		return dupBookIDs;
	}

	private boolean matchOtherBookOnDOIIdentifier( UniBook otherBook) {
		boolean result = false;
		
		String thisBookDOI = null;
		String otherBookDOI = null;
		for(Identifier id: getIdentifiers()) {
			String id_type = id.getName();

			if (  id_type.equalsIgnoreCase("doi") ) {    
				thisBookDOI = id.getValue();
				break;
			}
		}
		
		if ( thisBookDOI == null ) {
			return result;
		}
		
		for(Identifier id: otherBook.getIdentifiers()) {
			String id_type = id.getName();

			if (  id_type.equalsIgnoreCase("doi") ) {    
				otherBookDOI = id.getValue();
				break;
			}
		}
		
		if ( otherBookDOI == null ) {
			return result;
		}
		
		if ( thisBookDOI.equalsIgnoreCase(otherBookDOI)) {
			result = true;
		}

		return result;
	}

	public String getHoldingSummaryForBook(Connection conn) throws SQLException {
		String holding_summary = "";
		
		String query = "select * from ana_unified_book left join ana_unibook_holding on ana_unified_book.unibook_id=ana_unibook_holding.unibook_id where ana_unified_book.unibook_id=" + getUnibookID();
		try ( Statement stmt = conn.createStatement() ) {

			ResultSet rs = stmt.executeQuery( query );
			
			int au_count_archive = 0;
			int au_count_cmi_archive = 0;
			
			while ( rs.next() ) {
				String holding_id = rs.getString("holding_id");
				String source = rs.getString("source");
				
				if ( holding_id == null ) {		//no holding for this book
					holding_summary += "Commmitted only";
				}
				else {
					if ( source.equals("archive")) {
						
						au_count_archive ++;
					}
					else if ( source.equals("cmi and archive")) {
						au_count_cmi_archive++;
					}
				}
				
			}
			
			if ( au_count_archive > 0 ){
				holding_summary += "Not committed but archived in " + au_count_archive + " AUs";
			}
			else if ( au_count_cmi_archive > 0 ) {
				holding_summary += "Committed and archived in " + au_count_cmi_archive + " AUs";
			}
			

		}
		catch(SQLException sqle) {
			logger.error( programName + ": getHoldingSummaryForBook " + query + ". " + sqle.getMessage() );
			TDMUtil.printSQLException( sqle );
			throw sqle;
		}

		
		return holding_summary;
		
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
	public String getCreator() {
		return creator;
	}
	public void setCreator(String creator) {
		this.creator = creator;
	}
	public boolean isSeries() {
		return series;
	}
	public void setSeries(boolean series) {
		this.series = series;
	}
	public int getSeriesID() {
		return seriesID;
	}
	public void setSeriesID(int seriesID) {
		this.seriesID = seriesID;
	}
	public String getSeriesTitle() {
		return seriesTitle;
	}
	public void setSeriesTitle(String seriesTitle) {
		this.seriesTitle = seriesTitle;
	}
	public String getSeriesVolNo() {
		return seriesVolNo;
	}
	public void setSeriesVolNo(String seriesVolNo) {
		this.seriesVolNo = seriesVolNo;
	}
	public String getContentSetName() {
		return contentSetName;
	}
	public void setContentSetName(String contentSetName) {
		this.contentSetName = contentSetName;
	}
	public String getSortTitle() {
		return sortTitle;
	}
	public void setSortTitle(String sortTitle) {
		this.sortTitle = sortTitle;
	}
	public String getDisplayTitle() {
		return displayTitle;
	}
	public void setDisplayTitle(String displayTitle) {
		this.displayTitle = displayTitle;
	}
	public String getPubYear() {
		return pubYear;
	}
	public void setPubYear(String pubYear) {
		this.pubYear = pubYear;
	}
	public String getEdition() {
		return edition;
	}
	public void setEdition(String edition) {
		this.edition = edition;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getPublisherTitleFileName() {
		return publisherTitleFileName;
	}
	public void setPublisherTitleFileName(String publisherTitleFileName) {
		this.publisherTitleFileName = publisherTitleFileName;
	}
	public String getCreatedBy() {
		return createdBy;
	}
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
	public Timestamp getCreationTS() {
		return creationTS;
	}
	public void setCreationTS(Timestamp creationTS) {
		this.creationTS = creationTS;
	}
	public int getUnibookID() {
		return unibookID;
	}

	public Set<Identifier> getIdentifiers() {
		return identifiers;
	}

	public void setIdentifiers(Set<Identifier> identifiers) {
		this.identifiers = identifiers;
	}

	public String getProvider_id() {
		return provider_id;
	}

	public void setProvider_id(String providerID) {
		this.provider_id = providerID;
	}
	
	public String getProvider_name() {
		return provider_name;
	}

	public void setProvider_name(String provider_name) {
		this.provider_name = provider_name;
	}


	public Set<String> getHoldings() {
		return holdings;
	}

	public void setHoldings(Set<String> holdings) {
		this.holdings = holdings;
	}
	
	public String getSeriesISSN() {
		return seriesISSN;
	}

	public void setSeriesISSN(String seriesISSN) {
		this.seriesISSN = seriesISSN;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	
	

}

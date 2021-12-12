package org.portico.tdm.util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ithaka.portico.archive.client.ArchiveClient;
import org.ithaka.portico.archive.client.ArchiveClientException;
import org.ithaka.portico.archive.client.impl.ArchiveClientImpl;
import org.ithaka.portico.msgobjects.contenthandler.ContentInfo;
import org.portico.conprep.util.archiveexport.DbConnectionPool;
import org.portico.tdm.tdm2.tools.TDMUtil;


/**
 * This class uses ArchiveClient to export AUs into a designated directory.
 * Usage:
 * 
 * java org.portico.tdm.util.ExportAu  -book -publisher RIA -cs "RIA E-Books"   -since 01/01/1900 
 * java org.portico.tdm.util.ExportAu  -book -publisher RIA -cs "RIA E-Books"   --------export all RIA ebooks
 * java org.portico.tdm.util.ExportAu  -book -aufile book_au_list   -since 01/01/1900 -dest input/ebook/test
 * java org.portico.tdm.util.ExportAu  -book -au ark:/27927/phzfjsttjzr -dest /Users/dxie/eclipse-workspace/tdm2/input/ebook/newcontent_202111/muse 
 * 
 * (Not in use  java org.portico.tdm.util.ExportAu  "http://pr2ptgpprd04.ithaka.org:8095/repository/au/v1/locate" 3 1800000 60000 "jdbc:oracle:thin:@pr2ptcingestora02.ithaka.org:1525:PPARCH1" archivemd2ro archivemd2ro
 *    "E-Journal Content" "Single" "ark:/27927/phw6tnk6f8" "file:/Users/dxie/eclipse-workspace/tdm2/input/newcontent_202110/muse" "NA" "NA" "NA" "NA" )
 * @author dxie
 *
 */
public class ExportAu {
	
	
	final String ARMURL = "http://pr2ptgpprd04.ithaka.org:8095/repository/au/v1/locate";
	final String RETRY_ATTEMPTS = "3";
	final String READ_TIMEOUT = "1800000";
	final String CONNECTION_TIMEOUT = "60000";
	final String DATABASE_URL = "jdbc:oracle:thin:@pr2ptcingestora02.ithaka.org:1525:PPARCH1";
	final String DATABASE_USER = "archivemd2ro";
	final String DATABASE_PWD = "archivemd2ro";
	final String CONTENT_TYPE_JOURNAL = "E-Journal Content";
	final String CONTENT_TYPE_BOOK = "E-Book Content";
	final String DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";
	
	String afmUrl = "http://pr2ptgpprd04.ithaka.org:8095/repository/au/v1/locate";   	//args[0]
	int retryAttempts = 3;																//args[1]
	int readTimeout = 1800000;															//args[2]
	int connectionTimeout = 60000;														//args[3]
	String databaseurl = "jdbc:oracle:thin:@pr2ptcingestora02.ithaka.org:1525:PPARCH1";	//args[4]
	String databaseuser = "archivemd2ro";												//args[5]
	String databasepwd = "archivemd2ro";												//args[6]
	String contentType = null;															//args[7]		"E-Journal Content", "E-Book Content"
	String inputType = null;															//args[8]		'Single', 'List', 'ContentSet', 'ContentSetList'
	String inputValue = null;															//args[9]
	String destFolder = null;															//args[10]     	"file:/Users/dxie/eclipse-workspace/tdm2/input/newcontent_202110/muse"

	String ingestDateFrom = null;														//args[11]		'mm/dd/yyyy' or 'NA'
	String ingestDateTo = null;															//args[12]		'mm/dd/yyyy' or 'NA'

	String auContentModifiedDateFrom = null;											//args[13]		'mm/dd/yyyy' or 'NA'
	String auContentModifiedDateTo = null;												//args[14]		'mm/dd/yyyy' or 'NA'

	String driverName = "oracle.jdbc.driver.OracleDriver";
	int maxSize = 3;
	int maxAge = 3;
	
	static String programName = "ExportAu";
	static Logger logger = LogManager.getLogger(ExportAu.class.getName());
	
	String argStr;
	
	long starttime, endtime;
	String startTimeStr, endTimeStr;
	int auCount;
	
	String configDir = "config";
	String outputDir = "output";
	String inputDir = "input";
	String cacheDir = "data" + File.separator + "portico_cache";
	String subDir = "";						//ie input/newcontent_202104
	String logDir = "logs";
	String server = "PROD";
	String bookDir = "ebook";
	String otherDir = "other";
	
	String newContentFolder;				//newcontent_202110  or ebook/newcontent_202110
	String publisher;
	boolean bookFlag = false;

	String loadDateSince = null;			//in format of 03/01/2020. Default is last month first day. Use 01/01/1900 for beginning.
	String loadDateTo = null;				//in format of 03/01/2020. Default is this month first day

	private static DbConnectionPool aconPool = null;
	
	public ExportAu() {
		
		//get startTime
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		startTimeStr = dateFormat.format(cal.getTime());
		starttime = System.currentTimeMillis();
		

		auCount = 0;
		
		Calendar c = Calendar.getInstance(); 
		int this_month = c.get(Calendar.MONTH) + 1; // beware of month indexing from zero
		int this_year  = c.get(Calendar.YEAR);
		
		int last_month = this_month -1;
		int year = this_year;
		if( last_month == 0 ) {
			last_month =12;
			year = this_year-1;
		}
		
		//set default dates
		loadDateSince = ( last_month < 10? "0" + last_month: "" + last_month ) + "/01/" + year;	//03/01/2020
		loadDateTo = (this_month<10? "0" + this_month: "" + this_month ) + "/01/" + year;		//03/01/2021
				
		String contentdir = "newcontent_" + this_year + ( this_month<10? "0" + this_month: "" + this_month );		//default is for journals newcontent_202110
		setNewContentFolder(contentdir);
		
		
	}
	

	public static void main(String[] args) throws Exception {
		
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();
		
		ExportAu loader = new ExportAu();
		
		// parse the command line arguments
		try {
			CommandLine line = parser.parse( options, args );

			logger.info( programName + " with args: " + String.join(" ", args));
			loader.setArgStr( String.join(" ", args)) ;



			if ( line.hasOption("since")) {			//in format of 03/01/2021. If we want from beginning, use 01/01/1900. 
				String fromDate = line.getOptionValue("since");
				if ( fromDate.matches("^(0[1-9]|1[0-2])\\/(0[1-9]|1[0-9]|2[0-9]|3[0-1])\\/(20\\d{2}|1900)$")) {
					loader.setLoadDateSince(fromDate);
				}
				else {
					//default is last month first day
				}
			}


			if ( line.hasOption("to")) {				//in format of 09/01/2021
				String toDate = line.getOptionValue("to");

				if ( toDate.matches("[01][0-9]/[0123][0-9]/20\\d{2}")) {
					loader.setLoadDateTo(toDate);

					String contentdir = "newcontent_" + toDate.substring(6) + toDate.substring(0, 2)  ;   //newcontent_202109
					loader.setNewContentFolder(contentdir);
				}
				else {
					//default is this month first day
				}

			}
			
			if ( line.hasOption("dest")) {
				String dest = line.getOptionValue("dest");
				loader.setDestFolder(dest);		
			}



			//book type
			if ( line.hasOption("book")) {	
				loader.setBookFlag(true);
				loader.setNewContentFolder( "ebook" + File.separator + loader.getNewContentFolder());


				if ( line.hasOption("cs") && line.hasOption("publisher") ) {
					String cs = line.getOptionValue("cs");
					String publisher = line.getOptionValue("publisher");
					loader.loadBooksForAContentSet(cs, publisher);
				}
				else if ( line.hasOption("publisher")) {
					String publisher = line.getOptionValue("publisher");
					loader.loadBooksForAPublisher(publisher);
				}
				else if ( line.hasOption("publist")) {
					String publistfile = line.getOptionValue("publist");
					loader.loadBooksFromPublisherListFile(publistfile);
				}
				else if ( line.hasOption("au")) {
					String auid = line.getOptionValue("au");
					loader.loadBooksForAnAU(auid);                      //loadFrom and loadTo won't be used
				}
				else if ( line.hasOption("aulist")) {
					String aufile = line.getOptionValue("aulist");		//loadFrom and loadTo won't be used. aulist is under config/
					loader.loadBooksFromAuListFile(aufile);
				}


			}
			//journal type
			else if ( line.hasOption("publisher")) {
				String publisher = line.getOptionValue("publisher");
				loader.loadContentForAPublisher(publisher);   		
			}
			else if ( line.hasOption("cs")) {
				String cs = line.getOptionValue("cs");
				loader.loadContentForAContentSet(cs);        		
			}
			else if ( line.hasOption("cslist")) {
				String cslistfile = line.getOptionValue("cslist");
				loader.loadContentForContentSetList(cslistfile);        		
			}
			else if ( line.hasOption("publist")) {
				String publistfile = line.getOptionValue("publist");
				loader.loadContentFromPublisherListFile(publistfile);  
			}
			else if ( line.hasOption("au")) {
				String auid = line.getOptionValue("au");
				loader.loadContentForAnAU(auid);					//loadFrom and loadTo won't be used
			}
			else if ( line.hasOption("aulist")) {
				String aufile = line.getOptionValue("aulist");
				loader.loadContentForAuListFile(aufile);			//loadFrom and loadTo won't be used. aulist is under config/
			}



		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
		
	}	
		



	/**
	 * This is the main method to export a list of au_ids to a folder.
	 * @param archiveClient
	 * @param auIdList
	 * @param destFolder  	"file:/Users/dxie/eclipse-workspace/tdm2/input/newcontent_202110/muse"
	 * @throws Exception
	 */
	static void exportJournals(ArchiveClient archiveClient, List<String> auIdList, String destFolder) throws Exception{
		
		logger.info("-------- Start Exporting AUs to " + destFolder + " -------------");
		logger.info("Size of AuList == " + auIdList.size());
		
		try {
			for (String auId : auIdList) {
				logger.info("Exporting AU id="+auId);
				String auIdLastPart = auId.substring(auId.lastIndexOf('/') + 1);

				// get the zipped AU file from the archive
				ContentInfo contentInfo = null;
				try {
					contentInfo = archiveClient.getAUContent(auId, destFolder);
				} catch (Exception e) {
					logger.error( programName + ":exportJournals : Error exporting AU " + auId + " " + e.getMessage());
					e.printStackTrace();
				} 
				
				if (null == contentInfo) {
					logger.error(programName + ":exportJournals : Failed to get content for AU "+auId);
				} 
				else {
					// result path is a URI string eg "file:/tmp/58_dbc8jgj0m4.zip"
					String destinationPath = contentInfo.getDestinationPath();
					logger.info("destinationPath :"+destinationPath);
					URL zipFileUrl = new URL(destinationPath);
					URI zipFileUri = zipFileUrl.toURI();
					File zipFile = new File(zipFileUri);

					String auFolder = destFolder.substring(5) + "/" + auIdLastPart;
					logger.info("auFolder :"+auFolder);
					unZipIt(zipFile.getAbsolutePath(), auFolder);
					zipFile.delete();
				}
			}
			
			logger.info("-------- End Exporting -------------");
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	/**
	 * This class get AU Ids of a given content set by various selection parameters
	 * @param contentType
	 * @param contentSet
	 * @param ingestDateFrom
	 * @param ingestDateTo
	 * @param auContentModifiedDateFrom
	 * @param auContentModifiedDateTo
	 * @return
	 * @throws Exception
	 */
	private static List<String> getAuIdsByContentSet(String contentType, String contentSet, String ingestDateFrom, String ingestDateTo, String auContentModifiedDateFrom, String auContentModifiedDateTo) throws Exception {
		Connection acon = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<String> auList = new ArrayList<String>();
		try {

			logger.info("getting Archive connection");
			acon = aconPool.getConnection();
			logger.info("got Archive connection");

			String sql = "SELECT a.pmd_object_id FROM a_au a WHERE a.pmd_content_type = ? and a.pmd_content_set_name = ? ";

			if(ingestDateFrom != null && !ingestDateFrom.equalsIgnoreCase("NA")) {
				sql = sql + "and a.A_INGEST_TIMESTAMP >= to_date('"+ingestDateFrom+"','mm/dd/yyyy') ";
			}
			if(ingestDateTo != null && !ingestDateTo.equalsIgnoreCase("NA")) {
				sql = sql + "and a.A_INGEST_TIMESTAMP <= to_date('"+ingestDateTo+"','mm/dd/yyyy') ";
			}

			if(auContentModifiedDateFrom != null && !auContentModifiedDateFrom.equalsIgnoreCase("NA")) {
				sql = sql + "and a.A_CONTENT_MODIFIED_TIMESTAMP >= to_date('"+auContentModifiedDateFrom+"','mm/dd/yyyy') ";
			}
			if(auContentModifiedDateTo != null && !auContentModifiedDateTo.equalsIgnoreCase("NA")) {
				sql = sql + "and a.A_CONTENT_MODIFIED_TIMESTAMP <= to_date('"+auContentModifiedDateTo+"','mm/dd/yyyy') ";
			}

			logger.info("Content Set SQL ="+sql);

			pstmt = acon.prepareStatement(sql);
			pstmt.setString(1, contentType);
			pstmt.setString(2, contentSet);

			rs = pstmt.executeQuery();
			while (rs.next()) {
				auList.add(rs.getString(1));
			}

		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			aconPool.returnConnection(acon);
		}
		return auList;
	}

	private static void createFolder(String folder) {
		try {
			File temp = new File(folder);
			if (!temp.exists()) {
				temp.mkdirs();
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Unzip it
	 * @param zipFile input zip file
	 * @param output zip file output folder
	 */
	public static void unZipIt(String zipFile, String outputFolder) throws Exception{

		byte[] buffer = new byte[1024];
		System.out.println("Start UnZip : "+zipFile);
		// create output directory is not exists
		File folder = new File(outputFolder);
		if (!folder.exists()) {
			folder.mkdir();
		}

		// get the zip file content
		ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
		// get the zipped file list entry
		ZipEntry ze = zis.getNextEntry();

		while (ze != null) {

			String fileName = ze.getName();
			File newFile = new File(outputFolder + File.separator + fileName);

			//System.out.println("file unzip : " + newFile.getAbsoluteFile());

			// create all non exists folders
			// else you will hit FileNotFoundException for compressed folder
			new File(newFile.getParent()).mkdirs();

			FileOutputStream fos = new FileOutputStream(newFile);

			int len;
			/*while ((len = zis.read(buffer)) > 0) {
					fos.write(buffer, 0, len);
				}*/

			IOUtils.copy(zis, fos);
			if(null != fos) {
				fos.flush();
				fos.getFD().sync();
				fos.close();
			}
			ze = zis.getNextEntry();
		}

		zis.closeEntry();
		zis.close();

		System.out.println("End UnZip : "+zipFile);


	}
	
	
	//-------------------------------------------------------------------------------------------------------------------------------------------
	//Following methods are added by DX 2021.8.12
	//-------------------------------------------------------------------------------------------------------------------------------------------

	
	/**
	 * Export AUs for a list of content sets, these AUs were modified between fromDate and toDate.
	 * This is for journal content type
	 * The input is read from inputValue, the name of the file that contains the CS list, which has been defined already.
	 * @throws Exception 
	 */
	public void exportJournalAUFromCSList() throws Exception {
		
		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			//archiveClient = new ArchiveClientImpl(afmUrl,retryAttempts,readTimeout,connectionTimeout);
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":exportJournalAUFromCSList: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
		}
		
		// GET THE CONNECTION TO THE ARCHIVE MD DB
		aconPool = new DbConnectionPool(driverName, databaseurl, databaseuser, databasepwd,	maxSize, maxAge);


		//Read csList from inputValue
		List<String> csList = new ArrayList<String>();
		String filename = getInputValue();   //filename with path
		if ( filename == null ) {
			logger.error(programName + ":exportJournalAUFromCSList Input value not defined. ");
			throw new Exception("Wrong input parameter. Filename for content set list not defined for input value");
		}
		
		try {
			csList = TDMUtil.getLineContentFromFile(filename);
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error( programName + ":exportJournalAUFromCSList Error read from file " + filename  + " " + e.getMessage());
			throw e;
		} 

		List<String> auIdList = new ArrayList<String>();
		for (String contentSet: csList) {
			
			logger.info( programName + ":exportJournalAUFromCSList: export AUs for " + contentSet + " ...... " );
			auIdList.clear();
			auIdList.addAll(getAuIdsByContentSet(getContentType(), contentSet, getIngestDateFrom(), getIngestDateTo(), 
					getAuContentModifiedDateFrom(), getAuContentModifiedDateTo()));

			//Create contentSetFolder
			String contentSetFolder = getDestFolder() + File.separator + contentSet;
			// Take out 'file:' and create the folder
			createFolder(contentSetFolder.substring(5));
			
			try {
				//Export AUs for a journal
				exportJournals(archiveClient, auIdList, contentSetFolder);
			}
			catch(Exception e) {
				logger.error( programName + ":exportJournalAUFromCSList Error export AU for journal " + contentSet  + " " + e.getMessage());
				e.printStackTrace();
			}
		}

	}
	
	
	/**
	 * Export journal AUs of a content set that were modified between fromDate and toDate
	 * Called by ExportAu.loadContentForAContentSet()
	 * @throws Exception
	 */
	private void exportJournalAUForACS() throws Exception {
		
		//Initialize ArchiveClient
		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":exportJournalAUForACS: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
		}
		
		// GET THE CONNECTION TO THE ARCHIVE MD DB
		aconPool = new DbConnectionPool(driverName, databaseurl, databaseuser, databasepwd,	maxSize, maxAge);

		String csName = null;
		if (getInputType().equals("ContentSet")) {
			csName = getInputValue();
		}
		else {
			logger.error(programName + ":exportJournalAUForACS: Wrong input parameter ");
			throw new Exception("Wrong input parameter. Expect input type 'ContentSet'");
		}
		
		if (csName == null ) {
			logger.error(programName + ":exportJournalAUForACS: Wrong input parameter ");
			throw new Exception("Wrong input parameter. Content set name not defined for input value");
		}

		
		//Get AUs for this content set that modified between fromDate and toDate, save to auIdList
		List<String> auIdList = new ArrayList<>();

		logger.info( programName + ":exportJournalAUForACS: export AUs for " + csName + " ...... " );
		auIdList.clear();
		try {
			auIdList.addAll(getAuIdsByContentSet(getContentType(), csName, getIngestDateFrom(), getIngestDateTo(), 
					getAuContentModifiedDateFrom(), getAuContentModifiedDateTo()));
		} catch (Exception e1) {
			logger.error(programName + ":exportJournalAUForACS: Error getting AU ids for CS " + csName + " " + e1.getMessage());
			e1.printStackTrace();
		}

		//Create contentSetFolder
		String contentSetFolder = getDestFolder() + File.separator + csName;
		// Take out 'file:' and create the folder
		createFolder(contentSetFolder.substring(5));

		//Export AUs in auIdList to contentSetFolder
		try {
			exportJournals(archiveClient, auIdList, contentSetFolder);
		} catch (Exception e) {
			logger.error(programName + ":exportJournalAUForACS: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}



		
	}
	
	
	
	
	/**
	 * This method export book AUs from au list
	 * The filter of AUs are done here.
	 * Called by TDMPorticoNewContentLoader.loadNewBooksForAContentSet()
	 * Need to be replaced. Didn't do dedup.
	 * @param publisher
	 * @param publisherBookCSName 
	 */
	public void exportBookAUFromAUList_1(String publisher, String publisherBookCSName) throws Exception {
		
		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":exportBookAUFromAUList: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
		}
		
		// GET THE CONNECTION TO THE ARCHIVE MD DB
		aconPool = new DbConnectionPool(driverName, databaseurl, databaseuser, databasepwd,	maxSize, maxAge);

		String bookFileName = null;
		if (getInputType().equals("List")) {
			bookFileName = getInputValue();
		}
		else {
			logger.error(programName + ":exportBookAUFromAUList: Wrong input parameter ");
			throw new Exception("Wrong input parameter. Expect input type 'List'");
		}
		
		if (bookFileName == null ) {
			logger.error(programName + ":exportBookAUFromAUList: Wrong input parameter ");
			throw new Exception("Wrong input parameter. Book AU file name not defined for input value");
		}

		List<String> auIdList = new ArrayList<>();

		logger.info( programName + ":exportBookAUFromAUList: export AUs for " + publisherBookCSName + " ...... " );
		auIdList.clear();
		try {
			auIdList.addAll(getAuIdsByContentSet(getContentType(), publisherBookCSName, getIngestDateFrom(), getIngestDateTo(), 
					getAuContentModifiedDateFrom(), getAuContentModifiedDateTo()));
		} catch (Exception e1) {
			logger.error(programName + ":exportBookAUFromAUList: Error getting AU ids for " + bookFileName + " " + e1.getMessage());
			e1.printStackTrace();
		}
		
		
		//

		//Create contentSetFolder
		String contentSetFolder = getDestFolder() ;   //file:{PROJECT_DIR}/input/ebook/newcontent_202110/ria
		// Take out 'file:' and create the folder
		createFolder(contentSetFolder.substring(5));

		//Export AUs from Archive
		try {
			exportJournals(archiveClient, auIdList, contentSetFolder);
		} catch (Exception e) {
			logger.error(programName + ":exportBookAUFromAUList: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}


		
	}
	
	
	private void exportBookAUFromAUList(String publisher, String publisherBookCSName) throws Exception {
		
		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":exportBookAUFromAUList: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
		}
		
		// GET THE CONNECTION TO THE ARCHIVE MD DB
		aconPool = new DbConnectionPool(driverName, databaseurl, databaseuser, databasepwd,	maxSize, maxAge);

		String bookFileName = null;
		if (getInputType().equals("List")) {
			bookFileName = getInputValue();
		}
		else {
			logger.error(programName + ":exportBookAUFromAUList: Wrong input parameter ");
			throw new Exception("Wrong input parameter. Expect input type 'List'");
		}
		
		if (bookFileName == null ) {
			logger.error(programName + ":exportBookAUFromAUList: Wrong input parameter ");
			throw new Exception("Wrong input parameter. Book AU file name not defined for input value");
		}

		List<String> auIdList = new ArrayList<>();

		logger.info( programName + ":exportBookAUFromAUList: export AUs for " + publisherBookCSName + " ...... " );
		auIdList.clear();
		try {
			auIdList.addAll(getAuIdsByContentSet(getContentType(), publisherBookCSName, getIngestDateFrom(), getIngestDateTo(), 
					getAuContentModifiedDateFrom(), getAuContentModifiedDateTo()));
		} catch (Exception e1) {
			logger.error(programName + ":exportBookAUFromAUList: Error getting AU ids for " + bookFileName + " " + e1.getMessage());
			e1.printStackTrace();
		}
		
		
		//

		//Create contentSetFolder
		String contentSetFolder = getDestFolder() ;   //file:{PROJECT_DIR}/input/ebook/newcontent_202110/ria
		// Take out 'file:' and create the folder
		createFolder(contentSetFolder.substring(5));

		//Export AUs from Archive
		try {
			exportJournals(archiveClient, auIdList, contentSetFolder);
		} catch (Exception e) {
			logger.error(programName + ":exportBookAUFromAUList: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}


		
	}

	
	/**
	 * Export books from file which lists AUs
	 * loadFrom and loadTo dates won't be used. No Dedup process performed.
	 * @param aufile
	 * @throws Exception 
	 */
	private void loadBooksFromAuListFile(String aufile) throws Exception {

		logger.info( programName + ":loadBooksForAuListFile " + aufile  );


		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();


		List<String> bookAus = new ArrayList<>();
		
		try {
			bookAus = TDMUtil.getLineContentFromFile(configDir + File.separator + aufile);
		} catch (Exception e) {
			logger.error( programName + ":loadBooksFromAuListFile Error getting boo au_ids from file " + aufile + " " + e.getMessage());
			throw e;
		}
		

		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":loadBooksFromAuListFile: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
		}
		
		String destFolderUri = null;
		String proj_dir = System.getProperty("user.dir");
		
		
		String destFolder = getDestFolder();
		
		if ( destFolder != null  && !destFolder.startsWith("file:") && !destFolder.startsWith("/") ) {
			
			destFolderUri = "file:" + proj_dir + File.separator + destFolder;
		}
		else if (destFolder != null && destFolder.startsWith("/")  ) {
			destFolderUri = "file:" + destFolder;
		}
		else if ( destFolder != null && destFolder.startsWith("file:") ) {
			
		}
		else if ( destFolder == null ) {	//use default dir

			String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
			String subdir = new_content_dir + File.separator + otherDir;								//       ebook/newContent_202109/other
			String newContentInputDir = inputDir + File.separator + subdir;							// input/ebook/newContent_202109/other
			destFolderUri = "file:" + proj_dir + File.separator + newContentInputDir;				// file:/Users/dxie/eclipse-workspace/tdm2/input/ebook/newcontent_202109/other
		
		}
		
		destFolder = destFolderUri.substring(5);
		createFolder(destFolder);
		

		//check au count before exporting
		long au_count_before = scanDirForBookAuCount(destFolder);

		//Export AUs in auIdList to contentSetFolder
		try {
			exportJournals(archiveClient, bookAus, destFolderUri);
		} catch (Exception e) {
			logger.error(programName + ":loadBooksFromAuListFile: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}


		//after the exporting, get stats by scanning the newcontentdir
		long au_count = scanDirForBookAuCount(destFolder) - au_count_before;

		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
		String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));


		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadBooksFromAuListFile " + aufile );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + destFolder );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");

	}



	/**
	 * Export one AU. No time filter will be used. No de-dup performed.
	 * @param auid
	 * @throws Exception
	 */
	private void loadBooksForAnAU(String auid) throws Exception {
		
		logger.info( programName + ":loadBooksForAnAU " + auid  );


		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();


		List<String> bookAus = new ArrayList<>();
		bookAus.add(auid);
		

		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":loadBooksForAnAU: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
		}
		

		String destFolderUri = null;
		String proj_dir = System.getProperty("user.dir");
		
		
		String destFolder = getDestFolder();
		
		if ( destFolder != null  && !destFolder.startsWith("file:") && !destFolder.startsWith("/") ) {
			
			destFolderUri = "file:" + proj_dir + File.separator + destFolder;
		}
		else if (destFolder != null && destFolder.startsWith("/")  ) {
			destFolderUri = "file:" + destFolder;
		}
		else if ( destFolder != null && destFolder.startsWith("file:") ) {
			
		}
		else if ( destFolder == null ) {	//use default dir

			String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
			String subdir = new_content_dir + File.separator + otherDir;								//       ebook/newContent_202109/other
			String newContentInputDir = inputDir + File.separator + subdir;							// input/ebook/newContent_202109/other
			destFolderUri = "file:" + proj_dir + File.separator + newContentInputDir;				// file:/Users/dxie/eclipse-workspace/tdm2/input/ebook/newcontent_202109/other
		
		}
		
		destFolder = destFolderUri.substring(5);
		createFolder(destFolder);

		//check au count before exporting
		long au_count_before = scanDirForBookAuCount(destFolder);

		//Export AUs in auIdList to contentSetFolder
		try {
			exportJournals(archiveClient, bookAus, destFolderUri);
		} catch (Exception e) {
			logger.error(programName + ":loadBooksForAnAU: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}


		//after the exporting, get stats by scanning the newcontentdir
		long au_count = scanDirForBookAuCount(destFolder) - au_count_before;

		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
		String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));


		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadBooksForAnAU " + auid );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + destFolder );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
	}



	private void loadBooksFromPublisherListFile(String publistfile) throws Exception {
		
		logger.info( programName + ":loadNewBooksFromPublisherListFile ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_au_count = 0;
		
		
		List<String> publisher_list = new ArrayList<>();
		
		//get publisher list from publistfile
		try {
			publisher_list = TDMUtil.getLineContentFromFile(configDir + File.separator + publistfile);
		} catch (Exception e) {
			logger.error( programName + ":loadBooksFromPublisherListFile Error getting publisher ids from file " + publistfile + " " + e.getMessage());
			throw e;
		}
		
		int pub_count = publisher_list.size();
		int new_content_pub_count = 0;
		
		
		for(String publisher: publisher_list) {
			
			try {
				long au_count = loadBooksForAPublisher( publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_au_count += au_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":loadBooksFromPublisherListFile Error loading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        

		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewBooksFromPublisherListFile summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + pub_count + " have new content been exported");
		logger.info("Total exported " + total_au_count + " AUs "  );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		

	}



	private int loadBooksForAPublisher(String publisher) throws Exception {
		
		logger.info( programName + ":loadBooksForAPublisher " + publisher  );
		
		int exportedAuCount = 0;
		
		String publisherBookCSName = null;
		
		if ( publisher.equals("SAGE")) {
			publisherBookCSName = "SAGE KNOWLEDGE";
		}
		else {
			try {
				publisherBookCSName = TDMUtil.findPublisherBookContentSetName( publisher );
			} catch (Exception e) {
				logger.error( programName + ":loadBooksForAPublisher Error getting publisher book content set name " +  e.getMessage());
				throw e;
			}
		}
		
		try {
			exportedAuCount += loadBooksForAContentSet(publisherBookCSName, publisher);
		} catch (Exception e) {
			logger.error( programName + ":loadBooksForAPublisher " + publisher + " " + publisherBookCSName + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		if ( publisher.equals("SAGE")) {
			publisherBookCSName = "SAGE RESEARCH METHODS";
			try {
				exportedAuCount += loadBooksForAContentSet(publisherBookCSName, publisher);
			} catch (Exception e) {
				logger.error( programName + ":loadBooksForAPublisher " + publisher + " " + publisherBookCSName + " " + e.getMessage());
				e.printStackTrace();
				throw e;
			}
		}
		
		
		return exportedAuCount;

	}



	/**
	 * Export book AUs of a content set. 
	 * If no loadFrom and loadTo parameter is used, it will export deduped AUs in ANA_UNIBook_Holding table. If multiple AUs link to one unibook_id, then only 1 AU is exported.
	 * If loadFrom and loadTo parameters are used, filtered AUs will also be deduped using ana_unibook_holding table. 
	 * @param publisherBookCSName
	 * @param publisher
	 * @throws Exception
	 */

	public int loadBooksForAContentSet(String publisherBookCSName, String publisher) throws Exception  {

		logger.info( programName + ":loadBooksForAContentSet " + publisher + ": " + publisherBookCSName );


		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		String args = getArgStr();
		List<String> modifiedAus = null;
		if  ( ( args == null ) 	//called by external API call
				|| ( args!= null &&  args.indexOf("since") != -1 || args.indexOf("to") != -1 )) {
			
			String loadToDate = getAuContentModifiedDateTo();
			if ( loadToDate == null ) {
				loadToDate = getLoadDateTo();   //ie 03/02/2021
			}
			
			
			String loadFromDate = getAuContentModifiedDateFrom();
			if ( loadFromDate == null ) {
				loadFromDate = getLoadDateSince();
			}
			
			// GET THE CONNECTION TO THE ARCHIVE MD DB
			aconPool = new DbConnectionPool(driverName, databaseurl, databaseuser, databasepwd,	maxSize, maxAge);

			
			//get modified AUs in the date range
			try {
				modifiedAus = getAuIdsByContentSet(CONTENT_TYPE_BOOK, publisherBookCSName, "NA", "NA", loadFromDate, loadToDate);
			} catch (Exception e) {
				logger.error( programName + ":loadNewBooksForAContentSet  Error getting modified AUs for " + publisherBookCSName + " from " + loadFromDate + " to " + loadToDate +  " " + e.getMessage());
				e.printStackTrace();
			}
			
			if ( modifiedAus == null || modifiedAus.isEmpty()) {
				logger.info("-------------------------------------------------------------------------------------------");
				logger.info("-------------------------------------------------------------------------------------------");
				logger.info( programName + ":loadBooksForAContentSet No modified " + publisherBookCSName + " book between " + loadFromDate + " to " + loadToDate);
				logger.info("-------------------------------------------------------------------------------------------");
				logger.info("-------------------------------------------------------------------------------------------");
				return 0;
			}

		}
		
		List<String> bookAus = null;
		
	/*	try (Connection conn = TDMUtil.getConnection("PROD") ) {

			bookAus = TDMUtil.getDedupedBookListForAPublisher(conn, publisher, publisherBookCSName);

		} 
		catch (Exception e) {
			logger.error( programName + ":loadNewBooksForAContentSet  Error getting deduped book AUs for publisher " + publisher + " " + publisherBookCSName + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		} */
		
		//Find the overlap of the 2 lists (only keep deduped, newly modified AUs)
		//bookAus = filterByList(bookAus, modifiedAus);
		bookAus = filterDedupedBooks( modifiedAus, publisher, publisherBookCSName );
		
		
		//output to list_file_name
		String fileName =  configDir + File.separator + publisher.toLowerCase() + "_book_list";
		Path filePath = Paths.get( fileName );

		try {
			Files.write(filePath, bookAus);
		} catch (IOException e) {
			logger.error( programName + ":loadBooksForAContentSet Error write au list to file " + fileName + " "+ e.getMessage());
			throw e;
		}

		
		
		
		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":loadBooksForAnAU: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
			throw e1;
		}
		
		String destFolderUri = null;
		String proj_dir = System.getProperty("user.dir");
		
		
		String destFolder = getDestFolder();               //file:{PROJECT_DIR}/input/ebook/newcontent_202110/ria
		if ( destFolder != null  && destFolder.startsWith("file:") ) {
			destFolderUri = destFolder;
		}
		else if ( destFolder != null && !destFolder.startsWith("file:") && !destFolder.startsWith("/") ) {
			destFolderUri = "file:" + proj_dir + File.separator + destFolder;
		}
		else if (destFolder != null && destFolder.startsWith("/")  ) {
			destFolderUri = "file:" + destFolder;
		}
		else if ( destFolder == null ) {	//use default dir

			String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
			String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       ebook/newContent_202109/led
			String newContentInputDir = inputDir + File.separator + subdir;							// input/ebook/newContent_202109/led
			destFolderUri = "file:" + proj_dir + File.separator + newContentInputDir;			//file:{PROJECT_DIR}/input/ebook/newcontent_202110/ria
		
		}
		
		destFolder = destFolderUri.substring(5);
		createFolder(destFolder);

		//check au count before exporting
		long au_count_before = scanDirForBookAuCount(destFolder);

		try {
			exportJournals(archiveClient, bookAus, destFolderUri);
		}
		catch(Exception e) {
			logger.error( programName + ":loadBooksForAContentSet :exportJournals " + publisher + " " + e.getMessage());
			throw e;
		}

		//after the exporting, get stats by scanning the newcontentdir
		long au_count = scanDirForBookAuCount(destFolder) - au_count_before;

		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
		String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));


		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadBooksForAContentSet " + publisher + " " + publisherBookCSName);
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + destFolder );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
		return (int) au_count;

	}

	/**
	 * Only keep deduped book AUs from inputAus 
	 * @param inputBookAus
	 * @param publisherId
	 * @param publisherBookCSName
	 * @return
	 * @throws Exception 
	 */
	private List<String> filterDedupedBooks(List<String> inputBookAus, String publisherId, String publisherBookCSName) throws Exception {
		List<String> filteredBookAus = new ArrayList<>();
		
		
		
		try (Connection conn = TDMUtil.getConnection("PROD");
				Statement stmt = conn.createStatement();
				 ) {

			for( String bookAuId: inputBookAus ) {
				String query1 = "select a_ingest_timestamp, unibook_id from a_au, ana_unibook_holding where holding_id='" + bookAuId  + "' and a_au.pmd_object_id=ana_unibook_holding.holding_id";
				
				
				ResultSet rs = stmt.executeQuery(query1);
				
				String latest_ingest_timestamp = null;
				String ingest_timestamp = null;
				String unibook_id = null;
				
				//System.out.println(query1);
				
				if (rs.next()) {
					ingest_timestamp = rs.getString("a_ingest_timestamp");
					unibook_id = rs.getString("unibook_id");
				}
				else {
					logger.error( programName + ": filterDedupedBooks Error finding AU and unibook info for book Au Id " + bookAuId );
					continue;
				}
				
				String query2 = "select max(a_ingest_timestamp) from ana_unibook_holding, a_au where holding_id=a_au.pmd_object_id and unibook_id=" + unibook_id ;
				rs = stmt.executeQuery(query2);
				
				if ( rs.next()) {
					latest_ingest_timestamp = rs.getString("max(a_ingest_timestamp)");
				}
				
				if ( ingest_timestamp != null && ingest_timestamp.equals(latest_ingest_timestamp)) {
					//then this bookAuId should be used as deduped book id (we use latest ingested AU as the selected AU)
					filteredBookAus.add(bookAuId);
				}
				
				rs.close();
				
			}

		} 
		catch (Exception e) {
			logger.error( programName + ":filterDedupedBooks  Error getting deduped book AUs for publisher " + publisherId + " " + publisherBookCSName + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		
		return filteredBookAus;
	}


	/**
	 * This method finds the overlap of 2 lists
	 * @param bookAus
	 * @param modifiedAus
	 * @return
	 */
	private List<String> filterByList(List<String> bookAus, List<String> modifiedAus) {
		
		List<String> filteredList = bookAus.stream()
				  .filter(modifiedAus::contains)
				  .collect(Collectors.toList());
		
		
		return filteredList;
	}


	private void loadContentForAuListFile(String aufile) throws Exception {
		
		logger.info( programName + ":loadContentForAuListFile " + aufile  );


		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();

		List<String> aus = new ArrayList<>();
		
		try {
			aus = TDMUtil.getLineContentFromFile(configDir + File.separator + aufile);
		} catch (Exception e) {
			logger.error( programName + ":loadContentForAuListFile Error getting boo au_ids from file " + aufile + " " + e.getMessage());
			throw e;
		}
		

		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":loadContentForAuListFile: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
		}
		


		String destFolderUri = null;
		String proj_dir = System.getProperty("user.dir");
		
		
		String destFolder = getDestFolder();
		
		if ( destFolder != null && !destFolder.startsWith("file:") && !destFolder.startsWith("/") ) {
			
			destFolderUri = "file:" + proj_dir + File.separator + destFolder;
		}
		else if (destFolder != null && destFolder.startsWith("/")  ) {
			destFolderUri = "file:" + destFolder;
		}
		else if ( destFolder != null && destFolder.startsWith("file:") ) {
			
		}
		else if ( destFolder == null ) {	//use default dir

			String new_content_dir = getNewContentFolder();												//       newcontent_202109
			String subdir = new_content_dir + File.separator + otherDir;								//       newContent_202109/other
			String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/other
			destFolderUri = "file:" + proj_dir + File.separator + newContentInputDir;				// file:/Users/dxie/eclipse-workspace/tdm2/input/newcontent_202109/other
		
		}
		
		destFolder = destFolderUri.substring(5);
		createFolder(destFolder);

		
		//check au count before exporting
		long au_count_before = scanDirForJournalAuCount(destFolder);

		//Export AUs in auIdList to contentSetFolder
		try {
			exportJournals(archiveClient, aus, destFolderUri);
		} catch (Exception e) {
			logger.error(programName + ":loadContentForAuListFile: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}


		//after the exporting, get stats by scanning the newcontentdir
		long au_count = scanDirForJournalAuCount(destFolder) - au_count_before;

		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
		String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));


		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadContentForAuListFile " + aufile );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + destFolder );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");

	}


	/**
	 * This method export one AU. loadFrom and loadTo are not considered.
	 * @param auid
	 * @throws Exception 
	 */
	private void loadContentForAnAU(String auid) throws Exception {
		
		logger.info( programName + ":loadContentForAnAU " + auid  );


		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();


		List<String> aus = new ArrayList<>();
		aus.add(auid);
		

		ArchiveClient archiveClient = null;
		
		String serviceDiscoveryUrl="http://pr2ptgpprd01.ithaka.org:8763/eureka/apps";
		String applicationName="PR2_AFM";
		String locateAuRelativeURL="repository/au/v1/locate";
		
		try {
			archiveClient = new ArchiveClientImpl(serviceDiscoveryUrl,applicationName,locateAuRelativeURL,retryAttempts,readTimeout,connectionTimeout);
		} catch (ArchiveClientException e1) {
			logger.error( programName + ":loadContentForAnAU: Error create ArchiveClient " + e1.getMessage());
			e1.printStackTrace();
		}
		

		String destFolderUri = null;
		String proj_dir = System.getProperty("user.dir");
		
		
		String destFolder = getDestFolder();
		
		if ( destFolder != null && !destFolder.startsWith("file:") && !destFolder.startsWith("/") ) {
			
			destFolderUri = "file:" + proj_dir + File.separator + getDestFolder();
		}
		else if (destFolder != null && destFolder.startsWith("/")  ) {
			destFolderUri = "file:" + destFolder;
		}
		else if ( destFolder != null && destFolder.startsWith("file:") ) {
			
		}
		else if ( destFolder == null ) {	//use default dir

			String new_content_dir = getNewContentFolder();												//       newcontent_202109
			String subdir = new_content_dir + File.separator + otherDir;								//       newContent_202109/other
			String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/other
			destFolderUri = "file:" + proj_dir + File.separator + newContentInputDir;				// file:/Users/dxie/eclipse-workspace/tdm2/input/newcontent_202109/other
		
		}
		
		destFolder = destFolderUri.substring(5);
		createFolder(destFolder);

		//check au count before exporting
		long au_count_before = scanDirForJournalAuCount(destFolder);

		//Export AUs in auIdList to contentSetFolder
		try {
			exportJournals(archiveClient, aus, destFolderUri);
		} catch (Exception e) {
			logger.error(programName + ":loadContentForAnAU: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}


		//after the exporting, get stats by scanning the newcontentdir
		long au_count = scanDirForJournalAuCount(destFolder) - au_count_before;

		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
		String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));


		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadContentForAnAU " + auid );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + destFolder );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");

	}



	private void loadContentFromPublisherListFile(String publistfile) throws Exception {
		
		logger.info( programName + ":loadContentFromPublisherListFile ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_au_count = 0;
		
		
		List<String> publisher_list = new ArrayList<>();
		
		//get publisher list from publistfile
		try {
			publisher_list = TDMUtil.getLineContentFromFile(configDir + File.separator + publistfile);
		} catch (Exception e) {
			logger.error( programName + ":loadContentFromPublisherListFile Error getting publisher ids from file " + publistfile + " " + e.getMessage());
			throw e;
		}
		
		int pub_count = publisher_list.size();
		int new_content_pub_count = 0;
		
		
		for(String publisher: publisher_list) {
			
			try {
				long au_count = loadContentForAPublisher( publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_au_count += au_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":loadContentFromPublisherListFile Error loading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        

		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadContentFromPublisherListFile summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + pub_count + " have new content been exported");
		logger.info("Total exported " + total_au_count + " AUs "  );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");

	}



	/**
	 * Export AUs in the cs that were modified between loadFromDate and loadToDate
	 * @param cs
	 * @throws Exception 
	 */
	public void loadContentForAContentSet(String cs) throws Exception {

		logger.info( programName + ":loadContentForAContentSet " + cs );

		String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String loadFromDate = getLoadDateSince();

		String publisherId = TDMUtil.getPublisherIdfromContentSetName(cs);

		setContentType(CONTENT_TYPE_JOURNAL);
		setInputType("ContentSet");
		setInputValue(cs);


		String destFolderUri = null;
		String proj_dir = System.getProperty("user.dir");
		String destFolder = getDestFolder();
		
		if ( destFolder != null && !destFolder.startsWith("file:") && !destFolder.startsWith("/") ) {
			
			destFolderUri = "file:" + proj_dir + File.separator + destFolder;
		}
		else if (destFolder != null && destFolder.startsWith("/")  ) {
			destFolderUri = "file:" + destFolder;
		}
		else if ( destFolder != null && destFolder.startsWith("file:") ) {
			
		}
		else if ( destFolder == null ) {	//use default dir

			String new_content_dir = getNewContentFolder();												//       newcontent_202109
			String subdir = new_content_dir + File.separator + publisherId.toLowerCase();				//       newContent_202109/led
			String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/led
			destFolderUri = "file:" + proj_dir + File.separator + newContentInputDir;				// file:/Users/dxie/eclipse-workspace/tdm2/input/newContent_202109/led
		
		}
		
		destFolder = destFolderUri.substring(5);
		createFolder(destFolder);

		setIngestDateFrom("NA");
		setIngestDateTo("NA");
		setAuContentModifiedDateFrom(loadFromDate);
		setAuContentModifiedDateTo(loadToDate);

		try {
			exportJournalAUForACS();
		} catch (Exception e) {
			logger.error( programName + ":loadContentForAContentSet " + cs + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		

	}



	public long loadContentForAPublisher(String publisher) throws Exception {
		
		logger.info( programName + ":loadContentForAPublisher " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		//create cs file list, save to config/publisherID + "_cs_list";
		String csFileName = null;
		try (Connection conn = TDMUtil.getConnection("PROD") ) {

			try {
				csFileName = TDMUtil.createPublisherCSList(conn, publisher);		//csFileName is with path
			}
			catch(Exception e) {
				logger.error( programName + ":loadContentForAPublisher  " + publisher + " " + e.getMessage());
				throw e;
			}

			
		} 
		catch (Exception e1) {
			e1.printStackTrace();
			throw e1;
		}
		

		String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String loadFromDate = getLoadDateSince();
		
		String new_content_dir = getNewContentFolder();												//       newcontent_202109
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/led
		createFolder(newContentInputDir);
		
		long au_count_before = scanDirForJournalAuCount( newContentInputDir );		
		
		setContentType(CONTENT_TYPE_JOURNAL);
		setInputType("ContentSetList");
		setInputValue(csFileName);
		setIngestDateFrom("NA");
		setIngestDateTo("NA");
		setAuContentModifiedDateFrom(loadFromDate);
		setAuContentModifiedDateTo(loadToDate);
		
		try {
			exportJournalAUFromCSList();
		}
		catch(Exception e) {
			logger.error(programName + ":loadContentForAPublisher :exportJournalAUFromCSList   " + publisher + " " + e.getMessage());
			e.printStackTrace();
		}
		
		//after the exporting, get stats by scanning the newcontentdir
		long au_count = scanDirForJournalAuCount( newContentInputDir ) - au_count_before;
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadContentForAPublisher " + publisher );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + newContentInputDir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
		return au_count;
		
	}




	/**
	 * 
	 * @param cslistfile cslistfile is under config/ directory
	 * @return
	 */
	private int loadContentForContentSetList(String cslistfile) {
		
		logger.info( programName + ":loadContentForContentSetList " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String loadFromDate = getLoadDateSince();
		
		String new_content_dir = getNewContentFolder();												//       newcontent_202109
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/led
		long au_count_before = scanDirForJournalAuCount( newContentInputDir );		
		
		setContentType(CONTENT_TYPE_JOURNAL);
		setInputType("ContentSetList");
		setInputValue(configDir + File.separator  + cslistfile);
		setIngestDateFrom("NA");
		setIngestDateTo("NA");
		setAuContentModifiedDateFrom(loadFromDate);
		setAuContentModifiedDateTo(loadToDate);
		
		try {
			exportJournalAUFromCSList();
		}
		catch(Exception e) {
			logger.error(programName + ":loadContentForContentSetList :exportJournalAUFromCSList   " + publisher + " " + e.getMessage());
			e.printStackTrace();
		}
		
		//after the exporting, get stats by scanning the newcontentdir
		long au_count = scanDirForJournalAuCount( newContentInputDir ) - au_count_before;
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadContentForContentSetList " + publisher );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + newContentInputDir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
		return (int) au_count;
	}


	
	

	
	/**
	 * This method scans a publisher journal dir and returns the AU counts under each CS dir.
	 * The directory structure is input/newcontent_2021MM/publisher/CS/AU/.
	 * @param publisherDir input/newcontent_202109/led
	 * @return
	 */
	private long scanDirForJournalAuCount(String publisherDir) {
		
		int minDepth = 2;
		int maxDepth = 2;
		Path rootPath = Paths.get(publisherDir);
		int rootPathDepth = rootPath.getNameCount();
		long count = 0;
		
		try {
			 count = Files.walk(rootPath, maxDepth)
			        .filter(e -> e.toFile().isDirectory())
			        .filter(e -> e.getNameCount() - rootPathDepth >= minDepth)
			    //    .forEach(System.out::println)
			        .count()
			       ;
			
		} catch (IOException e) {
			logger.error( programName + ":scanDirForJournalAuCount " + publisherDir + " " + e.getMessage());
			e.printStackTrace();
		}
	

		
		return count;
	}
	


	/**
	 * This method scans a publisher book dir and returns the AU counts under each publisher dir.
	 * The directory structure is input/ebook/newcontent_202111/publisher/AU/.
	 * @param publisherDir input/ebook/newcontent_202109/muse
	 * @return
	 */
	private long scanDirForBookAuCount(String publisherDir) {
		int minDepth = 1;
		int maxDepth = 1;
		Path rootPath = Paths.get(publisherDir);
		int rootPathDepth = rootPath.getNameCount();
		long count = 0;
		
		try {
			 count = Files.walk(rootPath, maxDepth)
			        .filter(e -> e.toFile().isDirectory())
			        .filter(e -> e.getNameCount() - rootPathDepth >= minDepth)
			    //    .forEach(System.out::println)
			        .count()
			       ;
			
		} catch (IOException e) {
			logger.error( programName + ":scanDirForBookAuCount " + publisherDir + " " + e.getMessage());
			e.printStackTrace();
		}
	

		
		return count;
	}


	private static Options constructOptions() {
		
		final Options options = new Options();

		options.addOption(Option.builder("cs").required(false).hasArg().desc("Export AUs for this content set").build() );
		options.addOption(Option.builder("csfile").required(false).hasArg().desc("Export AUs for this content set list. csfile is under config/").build() );
		options.addOption(Option.builder("publisher").required(false).hasArg().desc("Export AUs for a publisher").build() );
		options.addOption(Option.builder("au").required(false).hasArg().desc("Export one single").build() );
		options.addOption(Option.builder("aulist").required(false).hasArg().desc("Export AUs for AUs in the file. aulist file is under config/").build() );
		
		options.addOption(Option.builder("book").required(false).desc("The content type is 'E-Book Content'").build() );
		
		
		options.addOption(Option.builder("since").required(false).hasArg().desc("Load modifiled AUs since this date, in format of MM/dd/yyyy.  Default is last month first day").build() );
		options.addOption(Option.builder("to").required(false).hasArg().desc("Load modifiled AUs to this date, in format of MM/dd/yyyy.  Default is this month first day").build() );
		options.addOption(Option.builder("dest").required(false).hasArg().desc("Destnation folder").build() );


		
		return options;
	}
	
	



	/*	Old main method.
 	public static void main(String[] args) throws Exception {
 		String afmUrl = args[0];
		int retryAttempts = Integer.parseInt(args[1]);
		int readTimeout = Integer.parseInt(args[2]);
		int connectionTimeout = Integer.parseInt(args[3]);
		String databaseurl = args[4];
		String databaseuser = args[5];
		String databasepwd = args[6];
		String contentType = args[7];
		String inputType = args[8];
		String inputValue = args[9];
		String destFolder = args[10];

		String ingestDateFrom = args[11];
		String ingestDateTo = args[12];

		String auContentModifiedDateFrom = args[13];
		String auContentModifiedDateTo = args[14];

		if(null != inputValue && inputValue.startsWith("ark:")) {
			inputType = "Single";
		}

		String driverName = "oracle.jdbc.driver.OracleDriver";
		int maxSize = 3;
		int maxAge = 3;

		ArchiveClient archiveClient = new ArchiveClientImpl(afmUrl,retryAttempts,readTimeout,connectionTimeout);
		// GET THE CONNECTION TO THE ARCHIVE MD DB
		aconPool = new DbConnectionPool(driverName, databaseurl, databaseuser, databasepwd,	maxSize, maxAge);

		List<String> auIdList = new ArrayList<String>();
		List<String> csList = new ArrayList<String>();

		boolean isActionContentSet = false;

		try {
			if (inputType.equals("Single")) {
				auIdList.add(inputValue.trim());
			} 
			else if (inputType.equals("List")) {
				File ip = new File(inputValue);
				FileReader reader = new FileReader(ip);
				BufferedReader br = new BufferedReader(reader);
				while (br.ready()) {
					auIdList.add(br.readLine().trim());
				}
				br.close();
			} 
			else if (inputType.equals("ContentSet")) {
				isActionContentSet = true;
				csList.add(inputValue.trim());
			} 
			else if (inputType.equals("ContentSetList")) {
				isActionContentSet = true;
				File ip = new File(inputValue);
				FileReader reader = new FileReader(ip);
				BufferedReader br = new BufferedReader(reader);
				while (br.ready()) {
					csList.add(br.readLine().trim());
				}
				br.close();
			}
			else {
				System.out.println("InputType not defined ="+inputType);
				return;
			}

			if(!isActionContentSet) {
				System.out.println("# of AUS "+auIdList.size());
				createFolder(destFolder.substring(5));
				exportJournals(archiveClient, auIdList, destFolder);
			} 
			else {

				for (String contentSet: csList) {
					auIdList.clear();
					auIdList.addAll(getAuIdsByContentSet(contentType, contentSet, ingestDateFrom, ingestDateTo, auContentModifiedDateFrom, auContentModifiedDateTo));
					//Create contentSetFolder
					String contentSetFolder = destFolder + File.separator + contentSet;
					// Take out 'file:' and create the folder
					createFolder(contentSetFolder.substring(5));
					exportJournals(archiveClient, auIdList, contentSetFolder);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		
	}
*/

	public String getAfmUrl() {
		return afmUrl;
	}

	public void setAfmUrl(String afmUrl) {
		this.afmUrl = afmUrl;
	}

	public int getRetryAttempts() {
		return retryAttempts;
	}

	public void setRetryAttempts(int retryAttempts) {
		this.retryAttempts = retryAttempts;
	}

	public int getReadTimeout() {
		return readTimeout;
	}

	public void setReadTimeout(int readTimeout) {
		this.readTimeout = readTimeout;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public String getDatabaseurl() {
		return databaseurl;
	}

	public void setDatabaseurl(String databaseurl) {
		this.databaseurl = databaseurl;
	}

	public String getDatabaseuser() {
		return databaseuser;
	}

	public void setDatabaseuser(String databaseuser) {
		this.databaseuser = databaseuser;
	}

	public String getDatabasepwd() {
		return databasepwd;
	}

	public void setDatabasepwd(String databasepwd) {
		this.databasepwd = databasepwd;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getInputType() {
		return inputType;
	}

	public void setInputType(String inputType) {
		this.inputType = inputType;
	}

	public String getInputValue() {
		return inputValue;
	}

	public void setInputValue(String inputValue) {
		this.inputValue = inputValue;
	}

	public String getDestFolder() {
		return destFolder;
	}

	public void setDestFolder(String destFolder) {
		this.destFolder = destFolder;
	}

	public String getIngestDateFrom() {
		return ingestDateFrom;
	}

	public void setIngestDateFrom(String ingestDateFrom) {
		this.ingestDateFrom = ingestDateFrom;
	}

	public String getIngestDateTo() {
		return ingestDateTo;
	}

	public void setIngestDateTo(String ingestDateTo) {
		this.ingestDateTo = ingestDateTo;
	}

	public String getAuContentModifiedDateFrom() {
		return auContentModifiedDateFrom;
	}

	public void setAuContentModifiedDateFrom(String auContentModifiedDateFrom) {
		this.auContentModifiedDateFrom = auContentModifiedDateFrom;
	}

	public String getAuContentModifiedDateTo() {
		return auContentModifiedDateTo;
	}

	public void setAuContentModifiedDateTo(String auContentModifiedDateTo) {
		this.auContentModifiedDateTo = auContentModifiedDateTo;
	}

	public String getDriverName() {
		return driverName;
	}

	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}



	public String getArgStr() {
		return argStr;
	}



	public void setArgStr(String argStr) {
		this.argStr = argStr;
	}



	public String getNewContentFolder() {
		return newContentFolder;
	}



	public void setNewContentFolder(String newContentFolder) {
		this.newContentFolder = newContentFolder;
	}



	public String getPublisher() {
		return publisher;
	}



	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}



	public boolean isBookFlag() {
		return bookFlag;
	}



	public void setBookFlag(boolean bookFlag) {
		this.bookFlag = bookFlag;
	}



	public String getLoadDateSince() {
		return loadDateSince;
	}



	public void setLoadDateSince(String loadDateSince) {
		this.loadDateSince = loadDateSince;
	}



	public String getLoadDateTo() {
		return loadDateTo;
	}



	public void setLoadDateTo(String loadDateTo) {
		this.loadDateTo = loadDateTo;
	}


	public long getStarttime() {
		return starttime;
	}


	public void setStarttime(long starttime) {
		this.starttime = starttime;
	}


	public long getEndtime() {
		return endtime;
	}


	public void setEndtime(long endtime) {
		this.endtime = endtime;
	}


	public String getStartTimeStr() {
		return startTimeStr;
	}


	public void setStartTimeStr(String startTimeStr) {
		this.startTimeStr = startTimeStr;
	}


	public String getEndTimeStr() {
		return endTimeStr;
	}


	public void setEndTimeStr(String endTimeStr) {
		this.endTimeStr = endTimeStr;
	}


	public int getAuCount() {
		return auCount;
	}


	public void setAuCount(int auCount) {
		this.auCount = auCount;
	}




}


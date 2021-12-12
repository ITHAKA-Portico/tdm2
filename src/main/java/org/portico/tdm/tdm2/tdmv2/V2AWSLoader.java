package org.portico.tdm.tdm2.tdmv2;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.datawarehouse.DWIssue;
import org.portico.tdm.tdm2.tools.TDMUtil;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * This class is used to combine V2 json files and upload them to AWS S3. Json files will be concatenated into json line files and be gzipped. 
 * Usage:
 * (linux copy-paste more_tdmenv first, or run . settdmenv.sh)
 * 	java org.portico.tdm.tdmv2.V2AWSLoader  -cs ISSN_02664674 -v 34 -n 1 -subdir camb
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -subdir sage -cs ISSN_00144851 -scanload  (30 articles)
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -subdir sage -cs ISSN_0013175  -scanload   (341  articles)
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -subdir sage -cs ISSN_00165492 -scanload   (1671  articles)
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -subdir sage -cs ISSN_00144851 -v 42 -n 3  (17 articles)
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -subdir sage -cs ISSN_00144851 -scanload -combineby 200
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -subdir led -publisher LED -combineby cs -scanload
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -subdir bond -publisher BOND -combineby 102
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -subdir camb -csfile camb_journal_aa -combineby 300 -scanload
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -book -subdir eup -publisher EUP -combineby 300 -scanload
 * 	java org.portico.tdm.tdmv2.V2AWSLoader -book -subdir eup -publisher EUP -aufile au_list
 *	java org.portico.tdm.tdmv2.V2AWSLoader -subdir newcontent_202104 -publisher newcontent_202004 -combineby 300 -scanload
 *
 *  java org.portico.tdm.tdm2.tdmv2.V2AWSLoader -subdir newcontent_202109/michigan -publisher newcontent_202009/michigan -combineby 300 -scanload
 * @author dxie
 *
 */
public class V2AWSLoader {
	
	static Properties props = new Properties();
	static Logger logger = LogManager.getLogger(V2AWSLoader.class.getName());
	static String programName = "V2AWSLoader";
	
	private static final String BUCKET = "ithaka-labs";
	private static final String V2_prefix = "tdm/v2/loading/portico/";
	S3AsyncClient client = null;
	
	//default value
	String outputDir = "output";
	String configDir = "config";
	String subDir = "";
	String server = "PROD";
	boolean bookType = false;
	boolean journalType = true;
	boolean scanload = false;
	String combineBy = null;			//Allowed values are: "cs", "issue", "publisher"?, or an integer size. By default, json files will be concatenated by content set. 		
	List<String> missingFileList = null;
	List<String> uploadedJsonlineFileList = null;
	int jsonFileCount;	
	
	long starttime, endtime;
	String startTimeStr, endTimeStr;
	String argStr;
	

	
	public V2AWSLoader() {
		
		missingFileList =new ArrayList<>();
		uploadedJsonlineFileList = new ArrayList<>();
		
		//client = S3AsyncClient.create();
		client =  S3AsyncClient.builder()
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                					.connectionMaxIdleTime(Duration.ofSeconds(5)))
                .build();
		
		//get startTime
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		startTimeStr = dateFormat.format(cal.getTime());
		starttime = System.currentTimeMillis();
		
		jsonFileCount = 0;
		
	}

	public static void main(String[] args) {
		
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();

		V2AWSLoader loader = new V2AWSLoader(  );

		// parse the command line arguments
		try {
			CommandLine line = parser.parse( options, args );
			String publisher;
			String cs;
			
			logger.info( programName + " with args: " + String.join(" ", args));
			loader.setArgStr( String.join(" ", args)) ;

			if ( line.hasOption("book")) {
				loader.setBookType(true);
				loader.setJournalType(false);
				loader.setOutputDir("output" + File.separator + "ebook");
			}
			else {
				loader.setJournalType(true);
				loader.setBookType(false);
				loader.setOutputDir("output");
			}

			if ( line.hasOption("subdir")) {		//required for all input
				loader.setSubDir(line.getOptionValue("subdir"));
			}
			else {
				V2AWSLoader.printUsage( options );
				System.exit(-1);
			}
			
			if ( line.hasOption("scanload")) {
				loader.setScanload(true);
			}
			else {
				loader.setScanload(false);
			}
			
			if ( line.hasOption("combineby")) {
				String combineByValue = line.getOptionValue("combineby");
				if ( combineByValue.equalsIgnoreCase("cs") 
					//	|| combineByValue.equalsIgnoreCase("volume")
						|| combineByValue.equalsIgnoreCase("issue")
						|| combineByValue.equals("publisher")) {
					
					loader.setCombineBy(combineByValue.toLowerCase() );
				}
				else if ( combineByValue.matches( "\\d+")) {		//ie every 100 json files be concatenated and zipped
					loader.setCombineBy(combineByValue);
				}
				else {
					loader.setCombineBy("cs");
				}
						
			}
			else {
				loader.setCombineBy("cs");
			}
			
			if ( line.hasOption("publisher") && ! line.hasOption("book")) {
				publisher = line.getOptionValue("publisher");

				loader.uploadV2ContentSetsForAPublisher( publisher );
				
				
			}
			else if ( line.hasOption("publisher") && line.hasOption("book") ) {
				
				publisher = line.getOptionValue("publisher");
				
				if ( line.hasOption("auid")) {
					String auid = line.getOptionValue("auid");
					
					loader.uploadV2ABookAU( publisher, auid, 1 );
					
				}
				else if (line.hasOption("aufile") ) {
					String aufilename = line.getOptionValue("aufile");

					loader.uploadV2BookAUsInAFile( publisher, aufilename );
				}
				else {
					loader.uploadV2BooksOfPublisher( publisher );
				}

				
			}
			
			//following are for journals
			else if ( line.hasOption("cs")) {
				cs = line.getOptionValue("cs");

				
				if ( line.hasOption("v") & line.hasOption("n")) {
					
					String volumeNo = line.getOptionValue("v");
					String issueNo = line.getOptionValue("n");
					
					//upload all articles' json files and upload them to AWS S3
					loader.uploadV2OneContentSetIssue( cs,  volumeNo, issueNo );		
					

				}
				else {

					loader.uploadV2OneContentSet( cs );

				}

				
			}
			else if ( line.hasOption("csfile")) {
				String filename = line.getOptionValue("csfile");

				loader.uploadV2ContentSetsInAFile( filename );

				
			}
			else if ( line.hasOption("issuefile")) {
				String filename = line.getOptionValue("issuefile");

				loader.uploadV2ContentSetIssuesInAFile( filename );

				
			}
			
			else {
				V2AWSLoader.printUsage( options );
			}
			
			loader.printSummary();


		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}


	}
	


	private void printSummary() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		endTimeStr = dateFormat.format(cal.getTime());
		endtime = System.currentTimeMillis();

        long totaltime = endtime -starttime;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
        //print stats
        logger.info("===============================================================");
        logger.info( programName + " with args: " + getArgStr());
        logger.info("Loading summary: " );
        logger.info("Total json files =" + jsonFileCount);
        logger.info("Successfully uploaded json line files (" + uploadedJsonlineFileList.size() + "): " );
        for( String jsonlinefile: uploadedJsonlineFileList ) {
        	logger.info( "\t" + jsonlinefile);
        }
        logger.info("Missing AU json files (" + missingFileList.size() + "): " + missingFileList.toString());
        
        for( String missingjsonfile: missingFileList ) {
        	logger.info( "\t" + missingjsonfile);
        }
        logger.info("Loading time " + startTimeStr + "[start] to " + endTimeStr + "[end], in total = " + timeUsedStr );
        logger.info("================================================================");
		
	}

	
	/**
	 * Upload all json files from publisher's book directory
	 * @param publisherID
	 * @return 
	 * @throws Exception 
	 */
	public int uploadV2BooksOfPublisher(String publisherID) throws Exception {
		
		logger.info(programName + ":uploadV2BooksOfPublisher: upload json files of " + publisherID );
		List<String> jsonlinefiles = new ArrayList<>();
		List<String> jsonFileList = null;
		
		boolean scanload = isScanload();
		String combineby = getCombineBy();
		
		if ( ! scanload ) {
			throw new Exception( "null scanload option not implemented yet ..." ) ;
		}
		
		//json files will be grouped using combineby value
		if ( combineby != null && combineby.matches("\\d+") ) {    //group json files by even size

			String subdir = getSubDir();
			String ebookdir = outputDir + File.separator + subdir;

			try {
				//scan all existing json files under publisher's ebook directory
				jsonFileList = scanDirectoryForJsonFiles( ebookdir );  
			} catch (IOException e) {
				logger.error( programName + ":uploadV2BooksOfPublisher: scanDirectoryForJsonFiles: " + ebookdir +  " " + e);
				throw e;
			}

			try {
				jsonlinefiles = concatenateBookJsonFilesBySize( publisherID, jsonFileList, combineby );
			}
			catch(Exception e) {
				logger.error( programName + ":uploadV2BooksOfPublisher:  concatenateBookJsonFilesBySize " + publisherID + e.getMessage() );
				throw e;
			}

		}

		//Gzip jsonlinefiles. gzipJsonLineFile has path 
		List<String> gzipJsonLineFile = gzipFiles( jsonlinefiles );  

		uploadFiles( gzipJsonLineFile );
		
		try {
			logBookUploadTimeToDB( publisherID, jsonFileList );
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2BooksOfPublisher " + e.getMessage());
			throw e;
		}
		

		//remove json files
		//cleanJsonFiles( jsonFileList );

		return jsonFileList.isEmpty()? 0: jsonFileList.size();

	}

	
	/**
	 * 
	 * @param publisher
	 * @param aufilename
	 * @throws Exception 
	 */
	private void uploadV2BookAUsInAFile(String publisher, String aufilename) throws Exception {
		
		List<String> bookAus = null;
		
		try {
			bookAus = TDMUtil.getListFromFile(aufilename);
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2BookAUsInAFile :getJournalsFromFile " + aufilename + ". " + e.getMessage());
			throw new Exception("Error getting book AUs");
		}

		for(int i =0; i< bookAus.size(); i++) {
			String auid = bookAus.get(i);
			try {
				uploadV2ABookAU(  publisher, auid, i );
			}
			catch(Exception e) {
				logger.error(programName + ":uploadV2BookAUsInAFile :uploadV2ABookAU " + auid + ". " + e.getMessage());
			}
		}

		
	}

	/**
	 * Take json file list from TDM_BOOK table of a book AU. Concatenate all json files into 1 json line file and compress it and upload it.
	 * No checking of combineby.
	 * @param publisherID
	 * @param auid
	 * @param i
	 * @throws Exception 
	 */
	private void uploadV2ABookAU(String publisherID, String auid, int i) throws Exception {

		List<String> jsonFileList = new ArrayList<>();

		String book_query = null;

		if ( publisherID.equalsIgnoreCase("ELGAR") || publisherID.equalsIgnoreCase("BENJAMINS")) {
			book_query = "select au_id from tdm_book where publisher_id='" + publisherID.toUpperCase() + "' and type='book' and au_id='" + auid + "'";
		}
		else {
			book_query = "select su_id from tdm_book where publisher_id='" + publisherID.toUpperCase() + "' and type='chapter' and au_id='" + auid + "' order by seq" ;
		}

		String dir = outputDir + File.separator + getSubDir();

		try (Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement(); ) {
			ResultSet rs = stmt.executeQuery(book_query);

			while ( rs.next()) {
				String id = rs.getString(1);
				jsonFileList.add( dir + File.separator +  id.replace("ark:/27927/", "") + ".json");
			}
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2ABookAU :Error getting json file list from TDM_BOOK table for " + publisherID + " " + auid + " " + e);
			throw e;
		}
		
		if ( jsonFileList.isEmpty()) {
			logger.error( programName + ":uploadV2ABookAU book/chapter json file doesn't exist " + auid);
			missingFileList.add(auid);
			throw new Exception("Json file doesnot exist");
		}

		String jsonlinefile = auid.replace("ark:/27927/", "") + ".jsonl" ;
		try {
			jsonlinefile = concatenateJsonFilesToOneFile( jsonFileList, jsonlinefile );
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2ABookAU:  concatenateJsonFilesToOneFile " + publisherID + " " + auid + " " + e);
			throw e;
		}

		//Gzip jsonlinefiles. gzipJsonLineFile has path 
		List<String> gzipJsonLineFile = gzipFiles( new ArrayList<String>(Arrays.asList(jsonlinefile)) );  

		uploadFiles( gzipJsonLineFile );

		try {
			logBookUploadTimeToDB( publisherID, jsonFileList );
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2ABookAU " + e.getMessage());
			throw e;
		}


		//remove json files
		//cleanJsonFiles( jsonFileList );


	}

	
	/**
	 * Possible combine by values: "cs", "issue", size
	 * @param publisher
	 * @throws Exception
	 */
	private void uploadV2ContentSetsForAPublisher(String publisher) throws Exception  {
		
		List<String> csnames = null;
		TDMUtil util = new TDMUtil();
		
		/*Connection conn = TDMUtil.getConnection("PROD");
		
		try {
			csnames = util.getArchivedJournalListForAPublisher(conn, publisher);
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2ContentSetsForAPublisher for " + publisher + ". " + e);
			throw e;
		}
		finally {
			conn.close();
		}*/
		
		//7/24/2020 scan publisher directory to get CS names
		csnames = scanPublisherDir(publisher);
		
		for( String cs: csnames ) { 
			
			try {
				uploadV2OneContentSet( cs );
			} catch (Exception e) {
				logger.error( programName + ":uploadV2ContentSetsForAPublisher :uploadV2OneContentSet" + publisher + " " + cs + " " + e);
				e.printStackTrace();
			}

		}
		
	}
	
	private List<String> scanPublisherDir(String publisher) {
		List<String> CSs = new ArrayList<>();
		
		String dir = outputDir + File.separator ;
		if ( isBookType()) {
			dir += "ebook" + File.separator + getSubDir() ;
		}
		else {
			dir += getSubDir();
		}
		
		File[] directories = new File(dir).listFiles(File::isDirectory);
		
		for(File subdir: directories) {
			String dirname = subdir.toString();
			String csname = dirname.substring(dirname.lastIndexOf(File.separator)+1);
			if ( csname.startsWith("ISSN")) {
				CSs.add(csname);
			}
		}
		
		Collections.sort(CSs);
		
		return CSs;
	}


	/**
	 * Concatenate all existing json files of a content set under subdir/ISSN_XXXX/ into one json line file and save this .jsonl file to subdir/. 
	 * Upload this .jsonl file to AWS S3. //Delete original .json files under subdir/ISSN_XXXX/
	 * Possible combineBy values: "issue", "size", "cs"
	 * @param cs
	 * @return The count of jsonl files that have been uploaded (not the gzip file count)
	 * @throws Exception 
	 */
	public int uploadV2OneContentSet(String cs) throws Exception {
		
		logger.info(programName + ":uploadV2OneContentSet: upload json files of " + cs );
		List<String> jsonlinefiles = new ArrayList<>();
		List<String> jsonFileList = null;

		boolean scanload = isScanload();
		String combineby = getCombineBy();

		//json files will be grouped using combineby value
		if ( combineby != null ) {
			if ( combineby.equalsIgnoreCase("issue")) {   //group json files by issue

				String subdir = getSubDir();
				String csdir = outputDir + File.separator + subdir + File.separator + cs;

				try {
					//scan all existing json files under csdir directory
					jsonFileList = scanDirectoryForJsonFiles( csdir );  
				} catch (IOException e) {
					logger.error( programName + ":findAllJsonFilesOfContentSet: scanDirectoryForJsonFiles: " + csdir +  " " + e.getMessage());
					throw e;
				}

				try {
					jsonlinefiles = concatenateJsonFilesByIssue( cs, jsonFileList  );
				}
				catch(Exception e) {
					logger.error( programName + ":uploadV2OneContentSet:  concatenateJsonFilesByIssue " + cs + e.getMessage() );
					throw e;
				}

			}
			else if ( combineby.equalsIgnoreCase("cs")) {   //group all jsons file in one batch

				try {
					//If scanload is true, just scan the directory of cs. If false, use DB query to find all AUs belong to this content set
					jsonFileList = findAllJsonFilesOfContentSet(cs, scanload );  
				} catch (Exception e) {
					logger.error( programName + ":uploadV2OneContentSet:  findAllJsonFilesOfContentSet " + cs + e.getMessage() );
					throw e;
				}

				//Concatenate json files into a List of 1 jsonlinefile (with path)
				Date snapshotDate = new Date();
				DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
				String tsStr = dateFormat.format(snapshotDate);

				String jsonlinefilename = cs + "_" + tsStr + ".jsonl";

				try {
					String jsonlinefileWithPath = concatenateJsonFilesToOneFile( jsonFileList, jsonlinefilename );

					jsonlinefiles.add(jsonlinefileWithPath);

				} catch (Exception e) {
					logger.error( programName + ":uploadV2OneContentSet :concatenateJsonFilesToOneFile : error concatenate " + jsonlinefilename + e );
					e.printStackTrace();
					throw e;
				}

			}
			else if ( combineby.matches("\\d+")) {    //group json files by even size

				String subdir = getSubDir();
				String csdir = outputDir + File.separator + subdir + File.separator + cs;
				
				try {
					//scan all existing json files under cs/ directory
					jsonFileList = scanDirectoryForJsonFiles( csdir );  
				} catch (IOException e) {
					logger.error( programName + ":findAllJsonFilesOfContentSet: scanDirectoryForJsonFiles: " + csdir +  " " + e.getMessage());
					throw e;
				}

				try {
					jsonlinefiles = concatenateJournalJsonFilesBySize( cs, jsonFileList, combineby );
				}
				catch(Exception e) {
					logger.error( programName + ":uploadV2OneContentSet:  concatenateJsonFileList " + cs + e.getMessage() );
					throw e;
				}
			}
		}

		//Gzip jsonlinefiles. gzipJsonLineFile has path 
		List<String> gzipJsonLineFile = gzipFiles( jsonlinefiles );  

		//upload gzipped jsonline files
		uploadFiles( gzipJsonLineFile );
		
		//get all AU ids from jsonFileList
		List<String> auList = new ArrayList<>();
		for(String jsonFileName: jsonFileList) {
			String au_id = "ark:/27927/" + jsonFileName.replaceAll(".*/", "").replace(".json", "");
			auList.add(au_id);
		}

		//log upload timestamp to TDM_BOOK table
		try   {
			logArticleUploadTimeToDB( auList );
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2OneContentSet " + e.getMessage());
			throw e;
		}
			

		//remove json files
		//	cleanJsonFiles( jsonFileList );


		return jsonlinefiles.isEmpty()? 0: jsonlinefiles.size();

	}
	

	private void logBookUploadTimeToDB(String publisherID, List<String> jsonFileList) {
		
		String update_query1 = "update tdm_book set upload_ts=current_timestamp where au_id=? and publisher_id='" + publisherID + "' and type='book'";
		String update_query2 = "update tdm_book set upload_ts=current_timestamp where su_id=? and publisher_id='" + publisherID + "' and type='chapter'";
		String update_query = null;
		
		if ( publisherID.equalsIgnoreCase("ELGAR") || publisherID.equalsIgnoreCase("BENJAMINS")) {
			update_query = update_query1;
		}
		else {
			update_query = update_query2;
		}
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				PreparedStatement update_stmt = conn.prepareStatement(update_query); ) {
			
			for(String jsonfile: jsonFileList) {
				String au_id = "ark:/27927/" + jsonfile.substring(jsonfile.lastIndexOf(File.separator) + 1 ).replace(".json", "" );
				
				update_stmt.setString(1, au_id);
				
				try {
					update_stmt.executeUpdate();
				}
				catch(SQLException e) {
					logger.error( programName + ":logBookUploadTimeToDB: Error updating TDM_BOOK:upload_ts for " + jsonfile + " " + e);
				}
			}
		}
		catch(  Exception  e) {
			logger.error( programName + ":logBookUploadTimeToDB:  " + e);
			e.printStackTrace();
		}
		
	}

	
	
	private void logArticleUploadTimeToDB(List<String> auList) {
		
		String update_query = "update tdm_au set upload_ts=current_timestamp where au_id=?";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				PreparedStatement update_stmt = conn.prepareStatement(update_query); ) {
			
			for(String au_id: auList) {
				//String au_id = "ark:/27927/" + jsonfile.substring(jsonfile.lastIndexOf("/") + 1 ).replace(".json", "" );
				
				update_stmt.setString(1, au_id);
				
				try {
					update_stmt.executeUpdate();
					//logger.info( programName + ":logArticleUploadTimeToDB " + update_query + " " + au_id );
				}
				catch(SQLException e) {
					logger.error( programName + ":logArticleUploadTimeToDB: Error updating TDM_AU:upload_ts for au_id " + au_id + " " + e);
				}
			}
		}
		catch(  Exception  e) {
			logger.error( programName + ":logArticleUploadTimeToDB:  " + e);
			e.printStackTrace();
		}
	}

	private List<String> concatenateBookJsonFilesBySize(String publisherID, List<String> jsonFileList,	String combinebysize ) throws Exception {
		
		List<String> jsonlineFileList = new ArrayList<>();
		
		if ( jsonFileList == null || jsonFileList.isEmpty()) {
			logger.error( programName + ":concatenateBookJsonFilesBySize : Empty input json files " + publisherID );
			throw new Exception("Empty input json file list");
		}
		
		int size = Integer.parseInt(combinebysize);

		
	    Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		String tsStr = dateFormat.format(snapshotDate);

		int groupCount = 0;
		if ( jsonFileList.size() % size == 0 ) {
			groupCount = jsonFileList.size()/size;
		}
		else {
			groupCount = jsonFileList.size()/size + 1;
		}
		
		int startIndex = 0;
		int endIndex = size;
		for( int i=0; i< groupCount; i ++ ) {
			
			if (endIndex > jsonFileList.size()) {
				endIndex = jsonFileList.size();
			}
			
			String jsonlinefilename = publisherID + "_Ebook_" + tsStr + "_" + i + ".jsonl";
			//get a sub list of jsonFileList
			List<String> sliceList = null;
			
			try {
				sliceList = jsonFileList.subList(startIndex, endIndex);
			}
			catch(IndexOutOfBoundsException e) {
				logger.error( programName + ":concatenateBookJsonFilesBySize :Error get sub list of json file list from " + startIndex + " to " + endIndex  );
				
				startIndex += size;
				endIndex += size;
				if (endIndex > jsonFileList.size()) {
					endIndex = jsonFileList.size();
				}
				
				continue;
			}

			try {
				String jsonlinefileWithPath = concatenateJsonFilesToOneFile( sliceList, jsonlinefilename );
				
				jsonlineFileList.add(jsonlinefileWithPath);
			} catch (Exception e) {
				logger.error( programName + ":concatenateBookJsonFilesBySize :concatenateJsonFilesToOneFile " + e );
				e.printStackTrace();
			}
			
			startIndex += size;
			endIndex += size;

		}
		
		
		return jsonlineFileList;
	}


	/**
	 * This method concatenate jsonFileList into json line files, each one contains combinebysize of json files
	 * @param cs
	 * @param jsonFileList
	 * @param combinebysize
	 * @return
	 * @throws Exception 
	 */
	private List<String> concatenateJournalJsonFilesBySize(String cs, List<String> jsonFileList, String combinebysize) throws Exception {
		List<String> jsonlineFileList = new ArrayList<>();
		
		if ( jsonFileList == null || jsonFileList.isEmpty()) {
			logger.error( programName + ":concatenateJournalJsonFilesBySize : Empty input json files " + cs );
			throw new Exception("Empty input json file list");
		}
		
		int size = new Integer(combinebysize).intValue();

		
	    Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		String tsStr = dateFormat.format(snapshotDate);

		int groupCount = 0;
		if ( jsonFileList.size() % size == 0 ) {
			groupCount = jsonFileList.size()/size;
		}
		else {
			groupCount = jsonFileList.size()/size + 1;
		}
		
		int startIndex = 0;
		int endIndex = size;
		for( int i=0; i< groupCount; i ++ ) {
			
			if (endIndex > jsonFileList.size()) {
				endIndex = jsonFileList.size();
			}
			
			String jsonlinefilename = cs + "_" + tsStr + "_" + i + ".jsonl";
			//get a sub list of jsonFileList
			List<String> sliceList = null;
			
			try {
				sliceList = jsonFileList.subList(startIndex, endIndex);
			}
			catch(IndexOutOfBoundsException e) {
				logger.error( programName + ":concatenateJournalJsonFilesBySize :Error get sub list of json file list from " + startIndex + " to " + endIndex  );
				
				startIndex += size;
				endIndex += size;
				if (endIndex > jsonFileList.size()) {
					endIndex = jsonFileList.size();
				}
				
				//continue;//????
			}

			try {
				String jsonlinefileWithPath = concatenateJsonFilesToOneFile( sliceList, jsonlinefilename );
				
				jsonlineFileList.add(jsonlinefileWithPath);
			} catch (Exception e) {
				logger.error( programName + ":concatenateJournalJsonFilesBySize :concatenateJsonFilesToOneFile " + e );
				e.printStackTrace();
			}
			
			startIndex += size;
			endIndex += size;

		}
		
		
		return jsonlineFileList;
	}

	/**
	 * This method groups jsonFileList by issues.
	 * @param cs
	 * @param jsonFileList
	 * @return List of json line file names with path
	 */
	private List<String> concatenateJsonFilesByIssue(String cs, List<String> jsonFileList) {
		List<String> jsonlineFileList = new ArrayList<>();
		
		Map<String, List<String>> issueId2JsonFiles = new HashMap<>();
	
		String article_issue_query = "select issn_no, vol_no, issue_no from dw_article where au_id=?";
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				PreparedStatement pstmt = conn.prepareStatement(article_issue_query); ) {
			
			for(String jsonfile: jsonFileList) {
				String auid = "ark:/27927/" + jsonfile.substring( jsonfile.lastIndexOf(File.separator)+1).replace(".json", "");
				
				pstmt.setString(1, auid);
				
				ResultSet rs = pstmt.executeQuery();
				if ( rs.next()) {
					String issn_no = rs.getString("issn_no");
					String vol_no =rs.getString("vol_no");
					String issue_no = rs.getString("issue_no");
					
					if ( issn_no.equalsIgnoreCase(cs)) {
						logger.error( programName + ":concatenateJsonFilesByIssue :json file " + jsonfile + " not belong to the content set " + cs );
						continue;
					}
					
					String issueid = issn_no + "_v" + vol_no.replaceAll("\\s+", "") + "_n" + issue_no.replaceAll("\\s+", "");
					
					if ( issueId2JsonFiles.containsKey(issueid)) {
						List<String> oneIssueJsonFiles = issueId2JsonFiles.get(issueid);
						if ( oneIssueJsonFiles == null ) {
							issueId2JsonFiles.put(issueid, new ArrayList<String>(Arrays.asList(jsonfile)));
						}
						else {
							oneIssueJsonFiles.add(jsonfile);
							issueId2JsonFiles.put(issueid, oneIssueJsonFiles);
						}
					}
					else {
						issueId2JsonFiles.put(issueid, new ArrayList<String>(Arrays.asList(jsonfile)));
					}
					
				}
				else {
					logger.error( programName + ":concatenateJsonFilesByIssue :cannot find which issue json file " + jsonfile + " belongs to " );
					continue;
				}
				
				rs.close();
				
			}
		}
		catch(Exception e) {
			logger.error( programName + ":concatenateJsonFilesByIssue :cannot find issues json files belongs to " );
			//TODO: stop or concatenate by cs?
		}
		
		//Now concatenate by key -values in issueId2JsonFiles
	    Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		String tsStr = dateFormat.format(snapshotDate);

		
		for (Map.Entry<String,List<String>> entry : issueId2JsonFiles.entrySet())  {
            String issueid = entry.getKey();
            List<String> oneIssueJsonFiles = entry.getValue();
            
            String jsonlinefilename = issueid + "_" + tsStr + ".jsonl";
            
            try {
				String jsonlinefileWithPath = concatenateJsonFilesToOneFile( oneIssueJsonFiles, jsonlinefilename );
				
				jsonlineFileList.add(jsonlinefileWithPath);
			} catch (Exception e) {
				logger.error( programName + ":concatenateJsonFilesByIssue :concatenateJsonFilesToOneFile : error concatenate " + jsonlinefilename + e );
				e.printStackTrace();
			}
		}
		
		return jsonlineFileList;
	}
	
	

	/**
	 * This method concatenate a list of json files into one json line file
	 * @param listOfJsonFiles
	 * @param jsonlinefilename The json line file without path
	 * @return The json line file with the path
	 * @throws Exception 
	 */
	private String concatenateJsonFilesToOneFile(List<String> listOfJsonFiles, String jsonlinefilename) throws Exception {
		
		String jsonlineFileWithPathName =  outputDir + File.separator + subDir + File.separator + jsonlinefilename;
		Path jsonlineFilePath = Paths.get(  jsonlineFileWithPathName );

	    // Charset for read and write
	    Charset charset = StandardCharsets.UTF_8;
	    
	    // Join files (lines)
	    for (String  jsonfile : listOfJsonFiles) {
	    	
	    	Path path = Paths.get( jsonfile );
	    	if (! Files.exists( path )) {
	    		logger.error( programName + ":concatenateJsonFilesToOneFile : File not exist " + jsonfile);
	    		missingFileList.add(jsonfile );
	    		continue;
	    	}
	    	
	        List<String> lines = null;
			try {
				lines = Files.readAllLines(path );
			} catch (IOException e1) {
				logger.error( programName + ":concatenateJsonFilesToOneFile : Error read lines from " + jsonfile + " " + e1.getClass().getSimpleName());
				throw new Exception("Error read json file");
			}
			
	        try {
				Files.write( jsonlineFilePath, lines, charset, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.error( programName + ":concatenateJsonFilesToOneFile : Error concatenate to " + jsonlineFileWithPathName + " " + e.getMessage());
				throw new Exception("Error appending to file");
			}
	        
	        jsonFileCount ++;
	    }
	    
	    return jsonlineFileWithPathName;
	    

	}

	private void uploadFiles(List<String> gzipJsonLineFile) {
		
		gzipJsonLineFile.stream().forEach(s-> {
			UploadGzipJsonLineFile( s );
		});
		
	}

	private List<String> gzipFiles(List<String> jsonlinefiles) {
		
/*		List<String> gzipfiles = new ArrayList<>();
		
		for(String jsonlinefilename: jsonlinefiles) {
		
			String gzipfile = TDMUtil.gzipFile( outputDir + File.separator + subDir + File.separator + jsonlinefilename );
			gzipfiles.add(gzipfile);
		}*/
		
		List<String> gzipfiles = jsonlinefiles
				.stream()
				.map(s-> { return TDMUtil.gzipFile( s ); })
				.collect(Collectors.toList());
		
		return gzipfiles;
	}

	/**
	 * 
	 * @param cs
	 * @param jsonFileList List of json files should be uploaded. They have full relative path.
	 * @param scanload If true, only concatenate jsonFileList. If false, need to report if any file is missing from jsonFileList. 
	 * @param combineby Define how jsonFileList will be concatenated, ie by cs, issue, volume, line size.
	 * @return List of concatenated json line file filenames with path, ie output/sage/V2_ISSN_XXXX_timestamp.jsonl
	 * @throws Exception 
	 */
/*	private List<String> concatenateJsonFileList(String cs, List<String> jsonFileList, boolean scanload, String combineby) throws Exception {
		
		List<String> jsonlinefiles = new ArrayList<>();
		
	    Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		String tsStr = dateFormat.format(snapshotDate);
		
		String subDir = getSubDir();
		String jsonlineFilePath = outputDir + File.separator + subDir + File.separator;
		int bySize = 0;

		if ( combineBy == null ) {		//all jsonFileList will be concatenated by cs
			String jsonlineFileNameWithPath = jsonlineFilePath + "V2_" + cs.replaceAll( "\\s+", "") + "_" + tsStr + ".jsonl";
			combineJsonFiles( cs, jsonFileList, scanload, jsonlineFileNameWithPath );
			jsonlinefiles.add(jsonlineFileNameWithPath);
		}
		else if ( combineBy.equals("issue")) {	//combine
			
		}
		else if ( combineBy.matches("\\d+")) {	//by size, ie 100
			bySize = new Integer(combineBy).intValue();
		}
		
		
		
		
		String jsonlineFileWithPathName =  outputDir + File.separator + subDir + File.separator + jsonlinefilename;
		Path jsonlineFilePath = Paths.get(  jsonlineFileWithPathName );


	    // Charset for read and write
	    Charset charset = StandardCharsets.UTF_8;
	    
	    // Join files (lines)
	    for (String  jsonfile : jsonFileList) {
	    	
	    	Path path = Paths.get( jsonfile );
	    	if ( ! scanload && ! Files.exists( path )) {
	    		logger.error( programName + ":concatenateJsonFileList : File not exist " + jsonfile);
	    		missingFileList.add(jsonfile );
	    		continue;
	    	}
	    	
	        List<String> lines = null;
			try {
				lines = Files.readAllLines(path );
			} catch (IOException e1) {
				logger.error( programName + ":concatenateJsonFileList : Error read lines from " + jsonfile + " " + e1.getClass().getSimpleName());
				throw new Exception("Error read json file");
			}
			
	        try {
				Files.write( jsonlineFilePath, lines, charset, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.error( programName + ":concatenateJsonFileList : Error concatenate to " + jsonlineFileWithPathName + " " + e.getMessage());
				throw new Exception("Error appending to file");
			}
	    }
	    
	    return jsonlinefiles;
	    

	}*/

/*	private void combineJsonFiles(String cs, List<String> jsonFileList, boolean scanload2, String jsonlineFileName) {
		
	    // Charset for read and write
	    Charset charset = StandardCharsets.UTF_8;
	    
	    // Join files (lines)
	    for (String  jsonfile : jsonFileList) {
	    	
	    	Path path = Paths.get( jsonfile );
	    	if ( ! scanload && ! Files.exists( path )) {
	    		logger.error( programName + ":concatenateJsonFileList : File not exist " + jsonfile);
	    		missingFileList.add(jsonfile );
	    		continue;
	    	}
	    	
	        List<String> lines = null;
			try {
				lines = Files.readAllLines(path );
			} catch (IOException e1) {
				logger.error( programName + ":concatenateJsonFileList : Error read lines from " + jsonfile + " " + e1.getClass().getSimpleName());
				throw new Exception("Error read json file");
			}
			
	        try {
				Files.write( jsonlineFilePath, lines, charset, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.error( programName + ":concatenateJsonFileList : Error concatenate to " + jsonlineFileWithPathName + " " + e.getMessage());
				throw new Exception("Error appending to file");
			}
	    }
	}*/

	/**
	 * Find all json files that will be uploaded for the content set
	 * @param cs
	 * @param scanload If true, just scan the directory of cs. If false, use DB query to find all AUs belong to this content set
	 * @return
	 * @throws Exception 
	 */
	private List<String> findAllJsonFilesOfContentSet(String cs, boolean scanload) throws Exception {
		List<String> jsonfileList = new ArrayList<>();
		
		String subdir = getSubDir();
		String csdir = outputDir + File.separator + subdir + File.separator + cs;
		
		if ( scanload ) {
			try {
				jsonfileList = scanDirectoryForJsonFiles( csdir );
			} catch (IOException e) {
				logger.error( programName + ":findAllJsonFilesOfContentSet: scanDirectoryForJsonFiles: " + csdir +  " " + e.getMessage());
				throw e;
			}
		}
		else {
			List<String> aulist = null;
			try {
				aulist = TDMUtil.queryDBForAllAUsOfContentSet( cs );
			} catch (Exception e) {
				logger.error( programName + ":findAllJsonFilesOfContentSet: queryDBForAllAUsOfContentSet: " + cs +  " " + e.getMessage());
				throw e;
			}
			
			jsonfileList = getJsonFileListFromAUList( aulist, cs );
		}

		return jsonfileList;
	}

	/**
	 * Find all json files under a content set directory
	 * @param dir
	 * @return json file names with full path
	 * @throws IOException 
	 */
	private List<String> scanDirectoryForJsonFiles(String dir ) throws IOException {
		List<String> jsonfiles = new ArrayList<>();
	
		try {
			try (Stream<Path> stream = Files.walk(Paths.get( dir ))) {
				jsonfiles = stream
						.filter( Files::isRegularFile)
						.filter( path -> path.toString().endsWith(".json"))
					//	.map(Path::getFileName)									//or getPath()?
						.map(Path::toString)
						.collect(Collectors.toList());
			}
		} catch(IOException e) {
			e.printStackTrace();
			logger.error( programName + ":scanDirectoryForJsonFiles: " + dir +  " " + e.getMessage());
			throw e;
		}
		
		return jsonfiles;
	}


	private List<String> getJsonFileListFromAUList(List<String> aulist, String cs) {
		
		String jsonfilepath = outputDir + File.separator + getSubDir() + File.separator + cs + File.separator;
		
		List<String> jsonfilelist = aulist.stream()
				.map( entry -> entry.substring(entry.lastIndexOf("/") + 1 ) + ".json" )
				.map( entry -> jsonfilepath + entry )
				.collect(Collectors.toList());
				
		return jsonfilelist;
	}

	/**
	 * Find out all de-duped articles of a content set issue. 
	 * Concatenate those article .json file into one json line file, save to subdir/.
	 * Upload this .jsonl file to AWS S3. Delete original .json files. 
	 * If article .json files are not presented under subdir/ISSN_XXXX/, missing AUs will be reported.
	 * We don't use combineBy when uploading by content set issue.
	 * @param cs
	 * @param volumeNo
	 * @param issueNo
	 * @throws Exception 
	 */
	private void uploadV2OneContentSetIssue(String cs, String volumeNo, String issueNo) throws Exception {
		
		//find out all de-duped article AUs of a content set issue
		String issueId = cs + " v." + volumeNo + " n." + issueNo;
		logger.info(programName + ":uploadV2OneContentSetIssue: upload json files of issue " + issueId );


		DWIssue issue = new DWIssue( cs, volumeNo, issueNo );
		List<String> articleAUs = null;

		try {
			articleAUs = issue.findAllArticleAUsInIssue( );
		} catch (Exception e) {
			e.printStackTrace();
			logger.error( programName + ":uploadV2OneContentSetIssue: findAllArticleAUsInIssue " + " " + issueId + " " + e.getMessage());
			throw e;
		}

		//convert from au list to json file list
		List<String> jsonfileList = getJsonFileListFromAUList( articleAUs, cs );

		//construct json line file name
		Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		String tsStr = dateFormat.format(snapshotDate);

		//V2_ISSN_XXXX_vVOLNO_nISSUENO_timestamp.jsonl ( doesn't have path)
		String jsonlinefilename = "V2_" + cs + "_v" + volumeNo.replaceAll("\\s", "") + "_n" + issueNo.replaceAll("\\s", "") + "_" + tsStr + ".jsonl";

		List<String> jsonlinefiles = new ArrayList<>();
		//concatenate json files of these AUs
		try {
			String jsonlinefileWithPath = concatenateJsonFilesToOneFile( jsonfileList, jsonlinefilename );

			jsonlinefiles.add(jsonlinefileWithPath);

		} catch (Exception e) {
			logger.error( programName + ":uploadV2OneContentSetIssue:  concatenateAUListForIssue " + cs + " v." + volumeNo + " n." + issueNo + " " + e );
			e.printStackTrace();
			throw e;
		}

		//Gzip jsonlinefiles. gzipJsonLineFile has path 
		List<String> gzipJsonLineFile = gzipFiles( jsonlinefiles );  

		uploadFiles( gzipJsonLineFile );

		try {
			logArticleUploadTimeToDB(articleAUs );
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2OneContentSetIssue " + e.getMessage());
			throw e;
		}
		
		//remove json files
		//cleanJsonFiles( jsonfileList );
		
		
	}


	private void cleanJsonFiles(List<String> jsonfileList) {
		
		jsonfileList.stream().forEach(s-> {
			try {
				Files.delete( Paths.get( s ) );
			} catch (IOException e) {
				logger.error( programName + ":cleanJsonFiles: Error deleting " + s + e.getMessage());
				
			}
		});
		
	}


	/**
	 * Upload one .jsonl.gz file to AWS S3 ithaka-labs/tdm/v2/loading/portico/
	 * The only difference to UploadOneJsonLineFile() is that this one sets contentEncoding("gzip")
	 * @param gzipJsonLineFile file name with path
	 */
	private void UploadGzipJsonLineFile(String gzipJsonLineFile) {
		
		String key = V2_prefix + Paths.get(gzipJsonLineFile).getFileName();
		
		logger.info( programName + ":UploadGzipJsonLineFile " + gzipJsonLineFile + " to S3 s3://" + BUCKET + "/" + key );

		CompletableFuture<PutObjectResponse> future = client.putObject(
				PutObjectRequest.builder()
				.bucket(BUCKET)
				.key(key)
				.contentEncoding("gzip")
				.build(),
				AsyncRequestBody.fromFile(Paths.get( gzipJsonLineFile))
		);
		
		future.whenComplete((resp, err) -> {

			if (resp != null) {
				//logger.info( programName + ":UploadObject " + resp  );
			} else {
				logger.error( programName + ":UploadGzipJsonLineFile Error uploading " + gzipJsonLineFile + " " + err.getMessage()  );
				throw new RuntimeException(err);
			}
		});

		future.join();
		
		uploadedJsonlineFileList.add(gzipJsonLineFile);
		
	}


	/**
	 * Upload one .jsonl file to AWS S3 ithaka-labs/tdm/v2/loading/portico/
	 * @param jsonLineFile file name with path
	 */
	private void UploadOneJsonLineFile(String jsonLineFile) {
		
		String key = V2_prefix + Paths.get(jsonLineFile).getFileName();
		
		logger.info( programName + ":UploadOneJsonLineFile " + jsonLineFile + " to S3 s3://" + BUCKET + "/" + key );

		CompletableFuture<PutObjectResponse> future = client.putObject(
				PutObjectRequest.builder()
				.bucket(BUCKET)
				.key(key)
				.build(),
				AsyncRequestBody.fromFile(Paths.get(jsonLineFile))
		);
		
		future.whenComplete((resp, err) -> {

			if (resp != null) {
				//logger.info( programName + ":UploadObject " + resp  );
			} else {
				logger.error( programName + ":UploadOneJsonLineFile Error uploading " + jsonLineFile + " " + err.getMessage()  );
				throw new RuntimeException(err);
			}
		});

		future.join();
		
		uploadedJsonlineFileList.add(jsonLineFile);
		
	}

	/**
	 * 
	 * @param cs
	 * @param volumeNo
	 * @param issueNo
	 * @param articleAUs
	 * @param scanload If scanload is true, missing json files will be saved in missingFileList
	 * @return json line file filename, ie "V2_ISSN_XXXX_vVOLNO_nISSUENO_timestamp.jsonl" 
	 * @throws Exception if Exception happens during concatenating. 
	 */
	private String concatenateAUListForIssue(String cs, String volumeNo, String issueNo, List<String> articleAUs, boolean scanload) throws Exception {
		
		
		String jsonlinefilename = null;	
		String subDir = getSubDir();
		
		Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		String tsStr = dateFormat.format(snapshotDate);
		
		//V2_ISSN_XXXX_vVOLNO_nISSUENO_timestamp.jsonl
		jsonlinefilename = "V2_" + cs + "_v" + volumeNo + "_n" + issueNo.replaceAll("\\s", "") + "_" + tsStr + ".jsonl";
		String jsonlineFileWithPathName =  outputDir + File.separator + subDir + File.separator + jsonlinefilename;
				
		Path jsonlineFilePath = Paths.get(  outputDir + File.separator + subDir + File.separator + jsonlinefilename );
		String jsonFilePath = outputDir + File.separator + subDir + File.pathSeparator + cs + File.separator;
		
	    // Charset for read and write
	    Charset charset = StandardCharsets.UTF_8;

	    // Join files (lines)
	    for (String  au_id : articleAUs) {
	    	String jsonfileWithPathName = jsonFilePath + au_id.substring(au_id.lastIndexOf("/") + 1 ) + ".json";
	    	Path path = Paths.get( jsonfileWithPathName);
	    	if ( scanload && ! Files.exists( path )) {
	    		logger.error( programName + ":concatenateAUListForIssue : File not exist " + jsonfileWithPathName);
	    		missingFileList.add(jsonfileWithPathName );
	    		continue;
	    	}
	    	
	        List<String> lines = null;
			try {
				lines = Files.readAllLines(path, charset);
			} catch (IOException e1) {
				logger.error( programName + ":concatenateAUListForIssue : Error read lines from " + jsonfileWithPathName + " " + e1.getMessage());
				throw new Exception("Error read json file");
			}
			
	        try {
				Files.write( jsonlineFilePath, lines, charset, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.error( programName + ":concatenateAUListForIssue : Error concatenate to " + jsonlineFileWithPathName + " " + e.getMessage());
				throw new Exception("Error appending to file");
			}
	    }

		return jsonlinefilename;
	}
	
	
	/**
	 * 
	 * @param filename file name under config/ directory
	 */
	private void uploadV2ContentSetIssuesInAFile(String filename) {
		List<String> lines = null;
		
		try {
			lines = TDMUtil.getLineContentFromFile(configDir + File.separator + filename);
		}
		catch(Exception e) {
			logger.error( programName + "uploadV2ContentSetIssuesInAFile: getJournalsFromFile " + filename + ". " + e.getMessage());
			return;
		}
		
		
		for(String line:lines) {
			if ( line.indexOf("\t") == -1 ) {
				logger.error(programName + "uploadV2ContentSetIssuesInAFile: invalid format of line " + line + ". Use content set name\tvolume number\tissue number.");
				continue;
			}
			String[] data = line.split("\t");
			
			String issn_no = data[0];
			String vol_no = data[1];
			String issue_no = data[2];
			
			if ( issn_no == null || vol_no == null || issue_no == null ) {
				logger.error(programName + "uploadV2ContentSetIssuesInAFile: invalid format of line " + line + ". Use content set name\tvolume number\tissue number.");
				continue;
			}
			
			try {
				uploadV2OneContentSetIssue(issn_no, vol_no, issue_no);
			} catch (Exception e) {
				logger.error(programName + "uploadV2ContentSetIssuesInAFile: Error uploading "+ issn_no + " v." + vol_no + " n." + issue_no + ". " + e.getMessage());
				continue;
			}

			
		}

		
		
	}

	
	/**
	 * Possible comebineBy values: "cs", "issue", "size"
	 * @param filename
	 */
	private void uploadV2ContentSetsInAFile(String filename) {
		
		List<String> csnames = null;
		
		try {
			csnames = TDMUtil.getLineContentFromFile(configDir + File.separator + filename);
		}
		catch(Exception e) {
			logger.error( programName + ":uploadV2ContentSetsInAFile:getJournalsFromFile " + filename + ". " + e.getMessage());
			return;
		}
		
		for( String cs: csnames ) { 
			
			try {
				uploadV2OneContentSet( cs );
			}
			catch(Exception e) {
				logger.error( programName + ":uploadV2ContentSetsInAFile: uploadV2OneContentSet " + cs + ". " + e.getMessage());
			}

		}
		
		
	}

	private static void printUsage(Options options) {
		
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "V2AWSLoader", options);  
		writer.flush();  
		
	}

	
	private static Options constructOptions() {

		final Options options = new Options();

		options.addOption(Option.builder("scanload").required(false).desc("Only upload what currently exist json files").build() );
		options.addOption(Option.builder("cs").required(false).hasArg().desc("Create json file for all AUs for a content set").build() );
		options.addOption(Option.builder("v").required(false).hasArg().desc("Create json file for a content set issue. Use with cs and n option").build() );
		options.addOption(Option.builder("n").required(false).hasArg().desc("Create json file for a content set issue. Use with cs and v option").build() );
		options.addOption(Option.builder("csfile").required(false).hasArg().desc("Create json file for all content sets in a file, one per line").build() );
		options.addOption(Option.builder("aufile").required(false).hasArg().desc("Create json file for all book AUs in a file, one per line").build() );
		options.addOption(Option.builder("issuefile").required(false).hasArg().desc("Create json file for issues supplied in the file").build() );
		options.addOption(Option.builder("subdir").required(true).hasArg().desc("The subdirectory name under input directory where AU files of Content Set have been stored.").build());
		options.addOption(Option.builder("publisher").required(false).hasArg().desc("Create json file for a publisher").build() );
		options.addOption(Option.builder("combineby").required(false).hasArg().desc("Concatenate output json file by issue or volume or content set or size").build() );
		
		options.addOption(Option.builder("book").required(false).desc("Indicate generate jons files for books").build() );
		
		return options;
	}

	public String getSubDir() {
		return subDir;
	}

	public void setSubDir(String subDir) {
		this.subDir = subDir;
	}

	public boolean isBookType() {
		return bookType;
	}

	public void setBookType(boolean bookType) {
		this.bookType = bookType;
	}

	public String getCombineBy() {
		return combineBy;
	}

	public void setCombineBy(String combineBy) {
		this.combineBy = combineBy;
	}

	public boolean isJournalType() {
		return journalType;
	}

	public void setJournalType(boolean journalType) {
		this.journalType = journalType;
	}

	public List<String> getMissingFileList() {
		return missingFileList;
	}

	public void setMissingFileList(List<String> missingFileList) {
		this.missingFileList = missingFileList;
	}

	public boolean isScanload() {
		return scanload;
	}

	public void setScanload(boolean scanload) {
		this.scanload = scanload;
	}

	public List<String> getUploadedJsonlineFileList() {
		return uploadedJsonlineFileList;
	}

	public void setUploadedJsonlineFileList(List<String> uploadedJsonlineFileList) {
		this.uploadedJsonlineFileList = uploadedJsonlineFileList;
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

	public String getArgStr() {
		return argStr;
	}

	public void setArgStr(String argStr) {
		this.argStr = argStr;
	}

	public int getJsonFileCount() {
		return jsonFileCount;
	}

	public void setJsonFileCount(int jsonFileCount) {
		this.jsonFileCount = jsonFileCount;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

}

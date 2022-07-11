package org.portico.tdm.tdm2.tools;


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.portico.tdm.tdm2.tdmv2.PorticoV2Processor;
import org.portico.tdm.tdm2.tdmv2.V2AWSLoader;
import org.portico.tdm.util.ExportAu;

/**
 * The purpose of this class is to automate exporting/processing/uploading new Portico journal content for TDM project.
 * To export new content from archive, use
 *  java org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -loadnew -since 03/01/2021 -to 09/01/2021 -publisher LED [-cs xxxx]
 * To process new content, use
 *  java org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -process  [-to 09/01/2021] [-publisher LED] [-cs xxxx]
 * To upload processed new content to AWS s3, use
 *  java org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -upload [-to 09/01/2021] [-publisher LED] [-cs xxxx]
 * To automate the whole process, use
 *  java org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -auto [-to 09/01/2021] [-publisher LED] [-cs xxxx]
 * To export new book content, use
 *  java org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -book -auto  -since 03/01/2020 -to 10/1/2021 -publisher RIA 
 *  
 *  
 *  
 *  
 *  (On server: after tdm2 command) 
 *  Compile command:  mvn clean compile assembly:single
 *  nohup $HOME/java/jdk-16.0.2/bin/java  -cp target/tdm2-0.0.1-SNAPSHOT-jar-with-dependencies.jar:config/:$CLASSPATH  org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -loadnew -since 03/01/2021 -to 09/01/2021 -publisher MICHIGAN > /dev/null  2>&1 &
 *  nohup $HOME/java/jdk-16.0.2/bin/java  -cp target/tdm2-0.0.1-SNAPSHOT-jar-with-dependencies.jar:config/:$CLASSPATH  org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -process -upload -since 03/01/2021 -to 09/01/2021 -publist pub_list > /dev/null  2>&1 &
 *  nohup $HOME/java/jdk-16.0.2/bin/java  -cp target/tdm2-0.0.1-SNAPSHOT-jar-with-dependencies.jar:config/:$CLASSPATH  org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -auto -book -since 03/01/2020 -to 10/01/2021 -publist pub_list > /dev/null  2>&1 &
 *  nohup $HOME/java/jdk-16.0.2/bin/java  -cp target/tdm2-0.0.1-SNAPSHOT-jar-with-dependencies.jar:config/:$CLASSPATH  org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -loadnew -book -since 01/01/2014 -to 02/01/2022 -publisher PRINCETON  > /dev/null  2>&1 &
 *  nohup java  -cp target/tdm2-0.0.1-SNAPSHOT-jar-with-dependencies.jar:config/:$CLASSPATH  org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -loadnew -since 01/01/2013 -to 03/01/2022 -publisher CJCMH  > /dev/null  2>&1 &
 *  nohup java  -cp target/tdm2-0.0.1-SNAPSHOT-jar-with-dependencies.jar:config/:$CLASSPATH  org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -loadnew -since 01/01/2014 -to 04/01/2022 -publisher PARTICLE  > /dev/null  2>&1 &
 *  nohup java  -cp target/tdm2-0.0.1-SNAPSHOT-jar-with-dependencies.jar:config/:$CLASSPATH  org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -process -upload -to 04/01/2022 -publisher PARTICLE  > /dev/null  2>&1 &
 *  java  -cp target/tdm2-0.0.1-SNAPSHOT-jar-with-dependencies.jar:config/:$CLASSPATH  org.portico.tdm.tdm2.tools.TDMPorticoNewContentLoader -loadnew -since beginning -to 04/01/2022 -publist publist
 *  (On server: after tdm2 command) 
 *  tdmloader -loadnew -since 03/01/2021 -to 09/01/2021 -publisher LED 
 *  tdmloader -auto -book -since 04/01/2022 -to 06/01/2022 -publist publist
 *  tdmloader -auto -since beginning -to 06/01/2022 -publisher BARATARIA
 *
 * 
 * @author dxie
 * @version 1.1	4/26/2022 Allow '-since beginning' parameter for cs , publisher, or publist
 *
 */
public class TDMPorticoNewContentLoader {
	
	static Properties props = new Properties();
	static Logger logger = LogManager.getLogger(TDMPorticoNewContentLoader.class.getName());
	static String programName = "TDMPorticoNewContentLoader";
	
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
	
	String argStr;
	
	String configDir = "config";
	String outputDir = "output";
	String inputDir = "input";
	String cacheDir = "data" + File.separator + "portico_cache";
	String subDir = "";						//ie input/newcontent_202104
	String logDir = "logs";
	String server = "PROD";
	String bookDir = "ebook";
	
	String newContentFolder;				//newcontent_202110  or ebook/newcontent_202110
	String publisher;
	boolean bookFlag = false;
	boolean sinceBeginningFlag = false;
	String loadDateSince = null;			//in format of 03/01/2020. Default is last month first day
	String loadDateTo = null;				//in format of 03/01/2020. Default is this month first day
	
	String journal_publisher_file = "Portico_TDM_J_Publishers.txt";
	
	List<String> portico_tdm_journal_publishers;
	List<String> portico_tdm_book_publishers;
	
	List<String> uploadedJsonlineFileList = null;
	int jsonFileCount;	
	
	long starttime, endtime;
	String startTimeStr, endTimeStr;
	int auCount;
	
	List<String> stats_list = null;

	
	public TDMPorticoNewContentLoader() {
		
		
		//get startTime
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		startTimeStr = dateFormat.format(cal.getTime());
		starttime = System.currentTimeMillis();
		
		jsonFileCount = 0;
		auCount = 0;
		
		Calendar c = Calendar.getInstance(); 
		int this_month = c.get(Calendar.MONTH) + 1; // beware of month indexing from zero
		int this_year  = c.get(Calendar.YEAR);
		
		int last_month = this_month -1;
		int last_year = this_year;
		if( last_month == 0 ) {
			last_month =12;
			last_year = this_year-1;
		}
		
		//set default dates
		loadDateSince = ( last_month < 10? "0" + last_month: "" + last_month ) + "/01/" + last_year;	//03/01/2020
		loadDateTo = (this_month<10? "0" + this_month: "" + this_month ) + "/01/" + this_year;		//03/01/2021
				
		String contentdir = "newcontent_" + this_year + ( this_month<10? "0" + this_month: "" + this_month );		//default is for journals newcontent_202110
		setNewContentFolder(contentdir);
		
		
		portico_tdm_journal_publishers = new ArrayList<>();
		portico_tdm_book_publishers = new ArrayList<>();
		stats_list = new ArrayList<>();

		
	}
	
	public static void main(String[] args) {
		
	
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();

		TDMPorticoNewContentLoader processor = new TDMPorticoNewContentLoader(  );

		// parse the command line arguments
		try {
			CommandLine line = parser.parse( options, args );
			
			logger.info( programName + " with args: " + String.join(" ", args));
			processor.setArgStr( String.join(" ", args)) ;
			
			
			
			if ( line.hasOption("since")) {			//in format of 03/01/2021
				String fromDate = line.getOptionValue("since");
				if ( fromDate.matches("^(0[1-9]|1[0-2])\\/(0[1-9]|1[0-9]|2[0-9]|3[0-1])\\/20\\d{2}$")) {
					processor.setLoadDateSince(fromDate);
				}
				else if ( fromDate.equalsIgnoreCase("beginning")) {
					processor.setSinceBeginningFlag(true);
				}
				else {
					//default is last month first day
				}
			}

			
			if ( line.hasOption("to")) {				//in format of 09/01/2021
				String toDate = line.getOptionValue("to");
				
				if ( toDate.matches("[01][0-9]/[0123][0-9]/20\\d{2}")) {
					processor.setLoadDateTo(toDate);
					
					String contentdir = "newcontent_" + toDate.substring(6) + toDate.substring(0, 2)  ;   //newcontent_202109
					processor.setNewContentFolder(contentdir);
				}
				else {
					//default is this month first day
				}

			}

			if ( line.hasOption("loadnew")) {			//export new content to input/newcontent_yyyyMM directory using Archive client
				
				//book type
				if ( line.hasOption("book")) {	
					processor.setBookFlag(true);
					processor.setNewContentFolder( "ebook" + File.separator + processor.getNewContentFolder());
					
					
					if ( line.hasOption("cs") && line.hasOption("publisher") ) {
						String cs = line.getOptionValue("cs");
						String publisher = line.getOptionValue("publisher");
						processor.loadNewBooksForAContentSet(cs, publisher);
					}
					else if ( line.hasOption("publisher")) {
						String publisher = line.getOptionValue("publisher");
						processor.loadNewBooksForAPublisher(publisher);
					}
					else if ( line.hasOption("publist")) {
						String publistfile = line.getOptionValue("publist");
						processor.loadNewBooksFromPublisherListFile(publistfile);
					}
					else {
						processor.loadNewBooksForAllPublishers();
					}
					
				}
				//journal type
				else if ( line.hasOption("publisher")) {
					String publisher = line.getOptionValue("publisher");
					processor.loadNewContentForAPublisher(publisher);
				}
				else if ( line.hasOption("cs")) {
					String cs = line.getOptionValue("cs");
					processor.loadNewContentForAContentSet(cs);
				}
				else if ( line.hasOption("publist")) {
					String publistfile = line.getOptionValue("publist");
					processor.loadNewContentFromPublisherListFile(publistfile);
				}
				else {
					processor.loadNewContentForAllPublishers();
				}
				
			}
			
			
			if ( line.hasOption("process")) {			//process content from input/newcontent_yyyyMM directory(default) , output to output/newcontent_yyyyMM directory, cache xml file, delete input AU folders

				//book type
				if ( line.hasOption("book")) {	
					processor.setBookFlag(true);
					processor.setNewContentFolder( processor.getNewContentFolder().startsWith("ebook")? processor.getNewContentFolder():"ebook" + File.separator + processor.getNewContentFolder());
					
					if ( line.hasOption("publisher")) {
						String publisher = line.getOptionValue("publisher"); 
						processor.processNewBooksForAPublisher(publisher);
					}
					else if ( line.hasOption("publist")) {
						String publistfile = line.getOptionValue("publist");
						processor.processNewBooksFromPublisherListFile(publistfile);
					}
					else {
						processor.processNewBooksForAllPublishers();
					}
					
				}
				//journal type
				else if ( line.hasOption("publisher")) {
					String publisher = line.getOptionValue("publisher");
					processor.processNewContentForAPublisher(publisher);
				}
				else if ( line.hasOption("cs")) {
					String cs = line.getOptionValue("cs");
					processor.processNewContentForAContentSet(cs);
				}
				else if ( line.hasOption("publist")) {
					String publistfile = line.getOptionValue("publist");
					processor.processNewContentFromPublisherListFile(publistfile);
				}
				else {
					processor.processNewContentForAllPublishers();
				}
				
			}
			
			
			if ( line.hasOption("upload")) {			//upload content from output/newcontent_yyyyMM directory(default) to AWS S3

				//book type
				if ( line.hasOption("book")) {	
					processor.setBookFlag(true);
					processor.setNewContentFolder( processor.getNewContentFolder().startsWith("ebook")? processor.getNewContentFolder():"ebook" + File.separator + processor.getNewContentFolder());
					
					if ( line.hasOption("cs") && line.hasOption("publisher") ) {
						String cs = line.getOptionValue("cs");
						String publisher = line.getOptionValue("publisher");
						processor.uploadNewBooksForAContentSet(cs, publisher);
					}
					else if ( line.hasOption("publisher")) {
						String publisher = line.getOptionValue("publisher");
						processor.uploadNewBooksForAPublisher(publisher);
					}
					else if ( line.hasOption("publist")) {
						String publistfile = line.getOptionValue("publist");
						processor.uploadNewBooksFromPublisherListFile(publistfile);
					}
					else {
						processor.uploadNewBooksForAllPublishers();
					}
					
				}
				//journal type
				else if ( line.hasOption("publisher")) {
					String publisher = line.getOptionValue("publisher");
					processor.uploadNewContentForAPublisher(publisher);
				}
				else if ( line.hasOption("cs")) {
					String cs = line.getOptionValue("cs");
					processor.uploadNewContentForAContentSet(cs);
				}
				else if ( line.hasOption("publist")) {
					String publistfile = line.getOptionValue("publist");
					processor.uploadNewContentFromPublisherListFile(publistfile);
				}
				else {
					processor.uploadNewContentForAllPublishers();
				}
				
			}
			
			if ( line.hasOption("auto")) {				//export new content + process + upload new content steps together
				
				if ( line.hasOption("publisher")) {
					String publisher = line.getOptionValue("publisher");
					
					if ( line.hasOption("book") ) {
						processor.setNewContentFolder( "ebook" + File.separator + processor.getNewContentFolder());
						
						processor.loadNewBooksForAPublisher(publisher);
						processor.processNewBooksForAPublisher(publisher);
						processor.uploadNewBooksForAPublisher(publisher);
					}
					else {
						processor.loadNewContentForAPublisher(publisher);
						processor.processNewContentForAPublisher(publisher);
						processor.uploadNewContentForAPublisher(publisher);
					}
					
					
				}
				else if ( line.hasOption("publist")) {
					String publistfile = line.getOptionValue("publist");
					
					if ( line.hasOption("book") ) {
						processor.setNewContentFolder( "ebook" + File.separator + processor.getNewContentFolder());
						
						processor.loadNewBooksFromPublisherListFile(publistfile);
						processor.processNewBooksFromPublisherListFile(publistfile);
						processor.uploadNewBooksFromPublisherListFile(publistfile);
					}
					else {
						processor.loadNewContentFromPublisherListFile(publistfile);
						processor.processNewContentFromPublisherListFile(publistfile);
						processor.uploadNewContentFromPublisherListFile(publistfile);
					}
					
				}
				else {
					
					if ( line.hasOption("book") ) {
						processor.setNewContentFolder( "ebook" + File.separator + processor.getNewContentFolder());
					
						processor.loadNewBooksForAllPublishers();
						
						processor.processNewBooksForAllPublishers();
					
						processor.uploadNewBooksForAllPublishers();
					}
					else {
						processor.loadNewContentForAllPublishers();
					
						processor.processNewContentForAllPublishers();
					
						processor.uploadNewContentForAllPublishers();
					}
				}
			}

		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}


			

	}
	

	/**
	 * Same as uploadNewBooksForAPublisher
	 * @param cs
	 * @param publisher
	 */
	private void uploadNewBooksForAContentSet(String cs, String publisher) {
		
		uploadNewBooksForAPublisher(publisher);
		
	}


	/**
	 * Combine json files by 500 to .jsonl.gz file and upload to s3://ithaka-labs/tdm/v2/loading/portico/. Also log upload time to tdm_au table.
	 * @param publisher
	 * @return
	 */
	private int uploadNewBooksForAPublisher(String publisher) {
		
		logger.info( programName + ":uploadNewBooksForAPublisher " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		

		String new_content_dir = getNewContentFolder();		//ebook/newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase(); 		//       ebook/newcontent_202109/led
		String newContentOutputDir = outputDir + File.separator + subdir;   				// output/ebook/newcontent_202110/led
		
	
		V2AWSLoader loader = new V2AWSLoader();
		//default is journal type
		loader.setScanload(true);
		loader.setSubDir(subdir);
		loader.setCombineBy( "500" );

		
		int jsonl_counter =0;
		

		try {
			jsonl_counter += loader.uploadV2BooksOfPublisher(publisher);
		}
		catch(Exception e) {
			logger.error( programName + ": uploadNewBooksForAPublisher Error uploading book content set jsonl files for " + publisher + " " + e.getMessage());
			if ( e.getMessage().indexOf("java.nio.file.NoSuchFileException") != -1 ) {
				logger.error( programName + ": uploadNewBooksForAPublisher Error uploading book content set jsonl files for " + publisher + " Empty content to upload");
			}
			else {
				e.printStackTrace();
			}
		}

		
		List<String> uploadedJsonlFiles = loader.getUploadedJsonlineFileList();
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":uploadNewBooksForAPublisher " + publisher );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total uploaded " + jsonl_counter + " jsonl files under under " + newContentOutputDir );
		for(String filename: uploadedJsonlFiles) {
			logger.info("\t\t" + filename);
		}
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":uploadNewBooksForAPublisher " + publisher );
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total uploaded " + jsonl_counter + " jsonl files under under " + newContentOutputDir );
		for(String filename: uploadedJsonlFiles) {
			stats_list.add("\t\t" + filename);
		}
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return jsonl_counter;
		
	}

	private void uploadNewBooksFromPublisherListFile(String publistfile) throws Exception {
		
		logger.info( programName + ":uploadNewBooksFromPublisherListFile ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_uploaded_jsonl_count = 0;
		
		
		List<String> publisher_list = new ArrayList<>();
		
		//get publisher list from publistfile
		try {
			publisher_list = TDMUtil.getLineContentFromFile(configDir + File.separator + publistfile);
		} catch (Exception e) {
			logger.error( programName + ":uploadNewBooksFromPublisherListFile Error getting publisher ids from file " + publistfile +  " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int pub_count = publisher_list.size();
		int processed_content_pub_count = 0;
		
		
		for(String publisher: publisher_list) {
			
			try {
				long jsonl_count = uploadNewBooksForAPublisher(publisher);
				
				if ( jsonl_count > 0 ) {
					processed_content_pub_count ++;
					total_uploaded_jsonl_count += jsonl_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":uploadNewBooksFromPublisherListFile Error uploading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
		
		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		String root_newcontentdir = outputDir + File.separator + new_content_dir ;					// output/ebook/newcontent_202009
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":uploadNewBooksFromPublisherListFile summary"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  processed_content_pub_count + " publishers out of " + pub_count + " has new content been uploaded");
		logger.info("Total uploaded " + total_uploaded_jsonl_count + " jsonl files under " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
	}

	private void uploadNewBooksForAllPublishers() throws Exception {
		
		logger.info( programName + ":uploadNewBooksForAllPublishers ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_uploaded_jsonl_count = 0;
		
		//all publisher list
		try {
			portico_tdm_book_publishers = TDMUtil.getTDMBookPublishers();
		} catch (Exception e) {
			logger.error( programName + ":uploadNewBooksForAllPublishers Error getting ebook publisher ids from tdm_publisher table. " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int portico_tdm_book_pub_count = portico_tdm_book_publishers.size();
		int processed_content_pub_count = 0;
		
		for(String publisher: portico_tdm_book_publishers) {
			
			try {
				long jsonl_count = uploadNewBooksForAPublisher(publisher);
				
				if ( jsonl_count > 0 ) {
					processed_content_pub_count ++;
					total_uploaded_jsonl_count += jsonl_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":uploadNewBooksForAllPublishers Error uploading books for publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		String root_newcontentdir = outputDir + File.separator + new_content_dir ;					// output/ebook/newcontent_202109
		
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":uploadNewBooksForAllPublishers summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  processed_content_pub_count + " publishers out of " + portico_tdm_book_pub_count + " have new content been processed");
		logger.info("Total uploaded " + total_uploaded_jsonl_count + " jsonl files under " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
	}



	private void uploadNewContentForAllPublishers() throws Exception {
		
		logger.info( programName + ":uploadNewContentForAllPublishers ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_uploaded_jsonl_count = 0;
		
		//all publisher list
		try {
			portico_tdm_journal_publishers = TDMUtil.getTDMJournalPublishers();
		} catch (Exception e) {
			logger.error( programName + ":uploadNewContentForAllPublishers Error getting publisher ids from tdm_publisher table. " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int portico_tdm_journal_pub_count = portico_tdm_journal_publishers.size();
		int new_content_pub_count = 0;
		
		for(String publisher: portico_tdm_journal_publishers) {
			
			try {
				long jsonl_count = uploadNewContentForAPublisher(publisher);
				
				if ( jsonl_count > 0 ) {
					new_content_pub_count ++;
					total_uploaded_jsonl_count += jsonl_count;
				}
			} catch (Exception e) {
				logger.error( programName + ":uploadNewContentForAllPublishers Error uploading jsonl files for publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
        String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String root_newcontentdir = outputDir + File.separator + "newcontent_" + loadToDate.substring(6) + loadToDate.substring(0,2) ;
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":uploadNewContentForAllPublishers summary"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + portico_tdm_journal_pub_count + " has new content been processed");
		logger.info("Total uploaded " + total_uploaded_jsonl_count + " jsonl files under " + root_newcontentdir );
		logger.info("------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
	}

	private void uploadNewContentFromPublisherListFile(String publistfile) throws Exception {

		logger.info( programName + ":uploadNewContentFromPublisherListFile ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_uploaded_jsonl_count = 0;
		
		
		List<String> publisher_list = new ArrayList<>();
		
		//get publisher list from publistfile
		try {
			publisher_list = TDMUtil.getLineContentFromFile(configDir + File.separator + publistfile);
		} catch (Exception e) {
			logger.error( programName + ":uploadNewContentFromPublisherListFile Error getting publisher ids from file " + publistfile +  " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int pub_count = publisher_list.size();
		int processed_content_pub_count = 0;
		
		
		for(String publisher: publisher_list) {
			
			try {
				long jsonl_count = uploadNewContentForAPublisher(publisher);
				
				if ( jsonl_count > 0 ) {
					processed_content_pub_count ++;
					total_uploaded_jsonl_count += jsonl_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":uploadNewContentFromPublisherListFile Error uploading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
        String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String root_newcontentdir = outputDir + File.separator + "newcontent_" + loadToDate.substring(6) + loadToDate.substring(0,2) ;
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":uploadNewContentFromPublisherListFile summary"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  processed_content_pub_count + " publishers out of " + pub_count + " has new content been uploaded");
		logger.info("Total uploaded " + total_uploaded_jsonl_count + " jsonl files under " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
		
	}

	private int uploadNewContentForAContentSet(String cs) throws Exception {
		
		logger.info( programName + ":uploadNewContentForAContentSet " + cs );
		
		//get publisher id;
		String publisher_id = null;
		try {
			publisher_id = TDMUtil.getPublisherIdfromContentSetName(cs);
		}
		catch(Exception e) {
			logger.error( programName + ":uploadNewContentForAContentSet  " + cs + " " + e.getMessage());
			throw e;
		}

		String new_content_dir = getNewContentFolder();			//newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher_id.toLowerCase();				//       newContent_202109/led
		//String newContentInputDir = inputDir + File.separator + subdir;  							// input/newContent_202109/led
		String newContentOutputDir = outputDir + File.separator + subdir;							//output/newContent_202109/led
		
		V2AWSLoader loader = new V2AWSLoader();
		//default is journal type
		loader.setScanload(true);
		loader.setSubDir(subdir);
		loader.setCombineBy("cs");
		
		int jsonl_counter =0;
		List<String> jsonlFiles = new ArrayList<>();

		try {
			jsonl_counter += loader.uploadV2OneContentSet(cs);
			jsonlFiles = loader.getUploadedJsonlineFileList();
		}
		catch(Exception e) {
			logger.error( programName + ": uploadNewContentForAContentSet Error uploading content set jsonl files " + cs + " " + e.getMessage());
			e.printStackTrace();
		}
	
		
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewContentForAContentSet " + cs );
		logger.info("Total uploaded " + jsonl_counter + " jsonl files under " + newContentOutputDir + "/" + cs );
		for(String filename: jsonlFiles) {
			logger.info("\t\t" + filename );
		}
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
		
		return jsonl_counter;
		
		
	}

	private int uploadNewContentForAPublisher(String publisher) {
		
		logger.info( programName + ":uploadNewContentForAPublisher " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		

		String new_content_dir = getNewContentFolder();			//newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//        newContent_202109/led
		String newContentOutputDir = outputDir + File.separator + subdir;							// output/newContent_202109/led
		
		//read all cs directory names
		List<String> csnames = new ArrayList<>();

		File[] directories = new File(newContentOutputDir).listFiles(File::isDirectory);
		
		for(File csdir: directories) {
			String dirname = csdir.toString();
			String csname = dirname.substring(dirname.lastIndexOf(File.separator)+1);

			csnames.add(csname);
		}
		
		Collections.sort(csnames);
		
		V2AWSLoader loader = new V2AWSLoader();
		//default is journal type
		loader.setScanload(true);
		loader.setSubDir(subdir);
		loader.setCombineBy("cs");

		
		int jsonl_counter =0;
		
		for(String csname: csnames) {
			try {
				jsonl_counter += loader.uploadV2OneContentSet(csname);

			}
			catch(Exception e) {
				logger.error( programName + ": uploadNewContentForAPublisher Error uploading one content set jsonl files " + csname + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		List<String> uploadedJsonlFiles = loader.getUploadedJsonlineFileList();
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":uploadNewContentForAPublisher " + publisher );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total uploaded " + jsonl_counter + " jsonl files under under " + newContentOutputDir );
		for(String filename: uploadedJsonlFiles) {
			logger.info("\t\t" + filename);
		}
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":uploadNewContentForAPublisher " + publisher );
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total uploaded " + jsonl_counter + " jsonl files under under " + newContentOutputDir );
		for(String filename: uploadedJsonlFiles) {
			stats_list.add("\t\t" + filename);
		}
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return jsonl_counter;
		
		
		
	}

	/**
	 * Process all tdm publisher's new content from input/newcontent_yyyyMM/publisher and save output json files to output/newcontent_yyyyMM
	 * New content are read from input/newcontent_yyyyMM/[pub id]/[csname]/AU
	 * Output json files are saved under output/newcontent_yyyyMM/[pub id]/[csname]/AU.json
	 * AU xml files will be cached to data/portico_cache_yyyyMM/AU/data/bits.xml, then origianl input AU folder will be deleted.
	 * TDM_AU entry will be inserted or updated.
	 * This method calls processNewContentForAPublisher(publisher) method.
	 * 
	 * @throws Exception
	 */
	private void processNewContentForAllPublishers() throws Exception {
		
		logger.info( programName + ":processNewContentForAllPublishers ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_processed_au_count = 0;
		
		//all publisher list
		try {
			portico_tdm_journal_publishers = TDMUtil.getTDMJournalPublishers();
		} catch (Exception e) {
			logger.error( programName + ":processNewContentForAllPublishers Error getting publisher ids from tdm_publisher table. " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int portico_tdm_journal_pub_count = portico_tdm_journal_publishers.size();
		int new_content_pub_count = 0;
		
		for(String publisher: portico_tdm_journal_publishers) {
			
			try {
				long au_count = processNewContentForAPublisher(publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_processed_au_count += au_count;
				}
			} catch (Exception e) {
				logger.error( programName + ":processNewContentForAllPublishers Error processsing publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
        String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String root_newcontentdir = inputDir + File.separator + "newcontent_" + loadToDate.substring(6) + loadToDate.substring(0,2) ;
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewContentForAllPublishers summary"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + portico_tdm_journal_pub_count + " have new content been processed");
		logger.info("Total processed " + total_processed_au_count + " AUs under " + root_newcontentdir );
		logger.info("------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
	}

	
	/**
	 * Process new content for publishers in publistfile, whose new content are stored under input/newcontent_yyyyMM/[pub id]/[csname]/AU
	 * Output json files are saved under output/newcontent_yyyyMM/[pub id]/[csname]/AU.json
	 * AU xml files will be cached to data/portico_cache_yyyyMM/AU/data/bits.xml, then origianl input AU folders will be deleted.
	 * TDM_AU entry will be inserted or updated.
	 * This method calls processNewContentForAPublisher(publisher) method.
	 * @param publistfile
	 * @throws Exception
	 */
	private void processNewContentFromPublisherListFile(String publistfile) throws Exception {
		
		logger.info( programName + ":processNewContentFromPublisherListFile ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_au_processed_count = 0;
		
		
		List<String> publisher_list = new ArrayList<>();
		
		//get publisher list from publistfile
		try {
			publisher_list = TDMUtil.getLineContentFromFile(configDir + File.separator + publistfile);
		} catch (Exception e) {
			logger.error( programName + ":processNewContentFromPublisherListFile Error getting publisher ids from file " + publistfile +  " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int pub_count = publisher_list.size();
		int processed_content_pub_count = 0;
		
		
		for(String publisher: publisher_list) {
			
			try {
				long au_count = processNewContentForAPublisher(publisher);
				
				if ( au_count > 0 ) {
					processed_content_pub_count ++;
					total_au_processed_count += au_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":processNewContentFromPublisherListFile Error loading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
        String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String root_newcontentdir = inputDir + File.separator + "newcontent_" + loadToDate.substring(6) + loadToDate.substring(0,2) ;
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewContentFromPublisherListFile Summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  processed_content_pub_count + " publishers out of " + pub_count + " has new content been processed");
		logger.info("Total processed " + total_au_processed_count + " AUs under " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
	}

	
	
	
	private int processNewContentForAContentSet(String cs) throws Exception {


		logger.info( programName + ":processNewContentForAContentSet " + cs );
		
		//get publisher id;
		String publisher_id = null;
		try {
			publisher_id = TDMUtil.getPublisherIdfromContentSetName(cs);
			
			setPublisher(publisher_id);
		}
		catch(Exception e) {
			logger.error( programName + ":processNewContentForAContentSet  " + cs + " " + e.getMessage());
			throw e;
		}

		String new_content_dir = getNewContentFolder();			//newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/led
		
		PorticoV2Processor p = new PorticoV2Processor();
				
		p.setSubDir(subdir);
		
		
		int processed_au_counter =0;
		

		try {
			processed_au_counter += p.processV2OneContentSetDir(cs);
		}
		catch(Exception e) {
			logger.error( programName + ": processNewContentForAContentSet Error processing one content set dir " + cs + " " + e.getMessage());
			e.printStackTrace();
		}
	
		
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewContentForAContentSet " + cs + "(" + publisher_id + ")");
		logger.info("Total processed " + processed_au_counter + " AUs under " + newContentInputDir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
		
		return processed_au_counter;
		
	}

	
	/**
	 * Process one publisher's new content from input/newcontent_yyyyMM/publisher and save output json files to output/newcontent_yyyyMM
	 * New content are stored under input/newcontent_yyyyMM/[pub id]/[csname]/AU
	 * Output json files are saved under output/newcontent_yyyyMM/[pub id]/[csname]/AU.json
	 * AU xml files will be cached to data/portico_cache_yyyyMM/AU/data/bits.xml, then origianl input AU folder will be deleted.
	 * TDM_AU entry will be inserted or updated.
	 * This method calls PorticoV2Processor.processV2OneContentSetDir(csname) method.
	 * 
	 * @param publisher
	 */
	private int processNewContentForAPublisher(String publisher) {

		logger.info( programName + ":processNewContentForAPublisher " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		

		String new_content_dir = getNewContentFolder();			//newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/led
		
		PorticoV2Processor p = new PorticoV2Processor();
		
		p.setSubDir(subdir);


		
		//read all cs directory names
		List<String> csnames = new ArrayList<>();
		List<String> processedCSList = new ArrayList<>();

		File[] directories = new File(newContentInputDir).listFiles(File::isDirectory);
		
		for(File csdir: directories) {
			String dirname = csdir.toString();
			String csname = dirname.substring(dirname.lastIndexOf(File.separator)+1);

			csnames.add(csname);
		}
		
		Collections.sort(csnames);
		
		int processed_au_counter =0;
		
		for(String csname: csnames) {
			try {
				int processed_au_count = p.processV2OneContentSetDir(csname);
				if ( processed_au_count> 0) {
					processed_au_counter += processed_au_count;
					processedCSList.add(csname);
				}
				
			}
			catch(Exception e) {
				logger.error( programName + ": processNewContentForAPublisher Error processing one content set dir " + csname + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewContentForAPublisher " + publisher );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total processed " + processed_au_counter + " AUs under " + newContentInputDir + " for following content sets:");
		for(String csname: processedCSList) {
			logger.info("\t\t" + csname);
		}
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":processNewContentForAPublisher " + publisher );
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total processed " + processed_au_counter + " AUs under " + newContentInputDir + " for following content sets:");
		for(String csname: processedCSList) {
			stats_list.add("\t\t" + csname);
		}
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return processed_au_counter;
		
		
	}
	
	
	/**
	 * Scan what is under input/ebook/newcontentdir/publisherdir/ and create json files to output/ebook/newcontentdir/publisherdir/bookAU.json
	 * @param publisher
	 * @return
	 */
	private int processNewBooksForAPublisher(String publisher) {
		
		logger.info( programName + ":processNewBooksForAPublisher " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		

		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       ebook/newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/ebook/newContent_202109/led
		
		PorticoV2Processor p = new PorticoV2Processor();
		
		p.setSubDir(subdir.replace(bookDir + File.separator, ""));

		p.setBookType(true);
		p.setJournalType(false);
		
		
		//scan for book AUs
		List<String> bookAUList = new ArrayList<>();
		File[] directories = new File(newContentInputDir).listFiles(File::isDirectory);
		
		if ( directories.length == 0) {
			logger.info(programName + ":processNewBooksForAPublisher " + publisher + " :No new content to process" );
			return 0;
		}
		
		for(File csdir: directories) {
			String dirname = csdir.toString();													//input/ebook/newcontent_202110/muse/phw14hnk755
			String bookAUDir = dirname.substring(dirname.lastIndexOf(File.separator)+1);		//phw14hnk755

			bookAUList.add( "ark:/27927/" + bookAUDir);
		}
		
		Collections.sort(bookAUList);
		
		boolean hasOA = false;
		try {
			if ( TDMUtil.isHasOABookPublisher(publisher)) {
				hasOA = true;
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		int i=1;
		for(String bookAU: bookAUList) {
			try {
				p.processV2ABookAU( publisher, bookAU, hasOA, i++ );
			} catch (Exception e) {
				logger.error( programName + ":processNewBooksForAPublisher :processV2ABookAU " + publisher + " " + bookAU + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		int processed_au_counter = p.getProcessedArticleOrBookList().size();
		
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		

		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewBooksForAPublisher " + publisher  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total processed " + processed_au_counter + " AUs under " + newContentInputDir  );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":processNewBooksForAPublisher " + publisher  );
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total processed " + processed_au_counter + " AUs under " + newContentInputDir  );
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return processed_au_counter;
		
		
	}

	private void processNewBooksFromPublisherListFile(String publistfile) throws Exception{
		
		
		logger.info( programName + ":processNewBooksFromPublisherListFile ");
		
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
			logger.error( programName + ":processNewBooksFromPublisherListFile Error getting publisher ids from file " + publistfile + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int pub_count = publisher_list.size();
		int new_content_pub_count = 0;
		
		
		for(String publisher: publisher_list) {
			
			try {
				long au_count = processNewBooksForAPublisher(publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_au_count += au_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":processNewBooksFromPublisherListFile Error process book for publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));

		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		String root_newcontentdir = inputDir + File.separator + new_content_dir ;					// input/ebook/newcontent_202109
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewBooksFromPublisherListFile summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + pub_count + " have new content been processed");
		logger.info("Total processed " + total_au_count + " AUs under " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
		
	}

	private void processNewBooksForAllPublishers() throws Exception {
		
		logger.info( programName + ":processNewBooksForAllPublishers ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_au_count = 0;
		
		//all publisher list
		try {
			portico_tdm_book_publishers = TDMUtil.getTDMBookPublishers();
		} catch (Exception e) {
			logger.error( programName + ":processNewBooksForAllPublishers Error getting publisher ids from tdm_publisher table. " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int portico_tdm_book_pub_count = portico_tdm_book_publishers.size();
		int new_content_pub_count = 0;
		
		for(String publisher: portico_tdm_book_publishers) {
			
			try {
				long au_count = processNewBooksForAPublisher(publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_au_count += au_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":processNewBooksForAllPublishers Error processing books for publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		String root_newcontentdir = inputDir + File.separator + new_content_dir ;					// input/ebook/newcontent_202109
		
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewBooksForAllPublishers summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + portico_tdm_book_pub_count + " have new content been processed");
		logger.info("Total processed " + total_au_count + " AUs under " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
	}
	
	
	
	/**
	 * Same as processNewBooksForAPublisher
	 * @param bookCSName
	 * @param publisher
	 * @return
	 */
/*	private int processNewBooksForAContentSet(String bookCSName, String publisher) {

		logger.info( programName + ":processNewBooksForAContentSet " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		

		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       ebook/newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/ebook/newContent_202109/led
		
		PorticoV2Processor p = new PorticoV2Processor();
		
		p.setSubDir(subdir.replace(bookDir + File.separator, ""));

		p.setBookType(true);
		p.setJournalType(false);
		
		
		//scan for book AUs
		List<String> bookAUList = new ArrayList<>();
		File[] directories = new File(newContentInputDir).listFiles(File::isDirectory);
		
		for(File csdir: directories) {
			String dirname = csdir.toString();
			String bookAUDir = dirname.substring(dirname.lastIndexOf(File.separator)+1);

			bookAUList.add( "ark:/27927/" + bookAUDir);
		}
		
		Collections.sort(bookAUList);
		
		int i=0;
		for(String bookAU: bookAUList) {
			try {
				p.processV2ABookAU( publisher, bookAU, i++ );
			} catch (Exception e) {
				logger.error( programName + ":processNewBooksForAContentSet :processV2BookOfAContentSet " + publisher + " " + bookCSName + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		int processed_au_counter = p.getProcessedArticleOrBookList().size();
		
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":processNewBooksForAContentSet " + publisher + " " + bookCSName );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total processed " + processed_au_counter + " AUs under " + newContentInputDir + " for " + bookCSName );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":processNewBooksForAContentSet " + publisher + " " + bookCSName );
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total processed " + processed_au_counter + " AUs under " + newContentInputDir + " for " + bookCSName );
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return processed_au_counter;
		
	}

*/
	
	
	private long loadNewBooksForAContentSet(String publisherBookCSName, String publisher) throws Exception {
		
		logger.info( programName + ":loadNewBooksForAContentSet " + publisher + ": " + publisherBookCSName );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
	
		String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String loadFromDate = getLoadDateSince();
		
		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       ebook/newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/ebook/newContent_202109/led
		
	
		ExportAu exportor = new ExportAu();
		

		String proj_dir = System.getProperty("user.dir");
		exportor.setDestFolder("file:" + proj_dir + File.separator + newContentInputDir );	//file:{PROJECT_DIR}/input/ebook/newcontent_202110/ria
		exportor.setIngestDateFrom("NA");
		exportor.setIngestDateTo("NA");
		exportor.setAuContentModifiedDateFrom(loadFromDate);
		exportor.setAuContentModifiedDateTo(loadToDate);
		
		int exportedAuCount = 0;
		try {
			exportedAuCount = exportor.loadBooksForAContentSet( publisherBookCSName, publisher);
		}
		catch(Exception e) {
			logger.error( programName + ":loadNewBooksForAContentSet :exportBookAUFromAUList " + publisher + " " + e.getMessage());
		}
		

		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewBooksForAContentSet " + publisher + " " + publisherBookCSName);
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + exportedAuCount + " AUs to " + newContentInputDir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":loadNewBooksForAContentSet " + publisher + " " + publisherBookCSName);
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total exported " + exportedAuCount + " AUs to " + newContentInputDir );
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return exportedAuCount;
		
	}
	
	
/*	private long loadNewBooksForAContentSet_1(String publisherBookCSName, String publisher) throws Exception {
		
		logger.info( programName + ":loadNewBooksForAContentSet " + publisher + ": " + publisherBookCSName );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
	
		//create AU file list, save to config/publisherID + "_ebook_list";
		String bookListFile = null;
		try (Connection conn = TDMUtil.getConnection("PROD") ) {

			bookListFile = createDedupedBookAUListForAContentSetFromANATable(conn, publisher, publisherBookCSName);
			
		} 
		catch (Exception e) {
			logger.error( programName + ":loadNewBooksForAContentSet  " + publisher + " " + publisherBookCSName + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String loadFromDate = getLoadDateSince();
		
		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       ebook/newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/ebook/newContent_202109/led
		
	
		ExportAu exportor = new ExportAu();
		
		exportor.setContentType(CONTENT_TYPE_BOOK);
		exportor.setInputType("List");
		exportor.setInputValue(bookListFile);
		String proj_dir = System.getProperty("user.dir");
		exportor.setDestFolder("file:" + proj_dir + File.separator + newContentInputDir );	//file:{PROJECT_DIR}/input/ebook/newcontent_202110/ria
		exportor.setIngestDateFrom("NA");
		exportor.setIngestDateTo("NA");
		exportor.setAuContentModifiedDateFrom(loadFromDate);
		exportor.setAuContentModifiedDateTo(loadToDate);
		
		try {
			exportor.exportBookAUFromAUList(publisher, publisherBookCSName);
		}
		catch(Exception e) {
			logger.error( programName + ":loadNewBooksForAContentSet :exportBookAUFromAUList " + publisher + " " + e.getMessage());
		}
		
		//after the exporting, get stats by scanning the newcontentdir
		long au_count = scanDirForAuCount(publisher, newContentInputDir);
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewBooksForAContentSet " + publisher + " " + publisherBookCSName);
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + newContentInputDir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":loadNewBooksForAContentSet " + publisher + " " + publisherBookCSName);
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total exported " + au_count + " AUs to " + newContentInputDir );
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return au_count;
		
	} */
	
	
	/**
	 * Export new book AUs to input/ebook/newcontent_202109/benjamins/. SAGE has 2 content sets to load.
	 * @param publisher
	 * @return
	 * @throws Exception
	 */
	private long loadNewBooksForAPublisher(String publisher) throws Exception {
		
		logger.info( programName + ":loadNewBooksForAPublisher " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		String publisherBookCSName = null;
		
		if ( publisher.equals("SAGE")) {
			publisherBookCSName = "SAGE KNOWLEDGE";
		}
		else {
			publisherBookCSName = TDMUtil.findPublisherBookContentSetName( publisher );
		}
		
		//after the exporting, get stats by scanning the newcontentdir
		long au_count = 0;
		
		try {
			au_count = loadNewBooksForAContentSet(publisherBookCSName, publisher);
		}
		catch(Exception e) {
			logger.error( programName + ":loadNewBooksForAPublisher " +   publisherBookCSName + " " + publisher + " " + e.getMessage());
		}
		
		if ( publisher.equals("SAGE")) {
			publisherBookCSName = "SAGE RESEARCH METHODS";
			try {
				au_count = loadNewBooksForAContentSet(publisherBookCSName, publisher);
			}
			catch(Exception e) {
				logger.error( programName + ":loadNewBooksForAPublisher " +  publisherBookCSName + " " + publisher + " " + e.getMessage());
			}
		}
		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewBooksForAPublisher " + publisher );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs  "  );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":loadNewBooksForAPublisher " + publisher );
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total exported " + au_count + " AUs  "  );
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return au_count;
	}



	public void loadNewBooksFromPublisherListFile(String publistfile) throws Exception {
		
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
			logger.error( programName + ":loadNewBooksFromPublisherListFile Error getting publisher ids from file " + publistfile + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int pub_count = publisher_list.size();
		int new_content_pub_count = 0;
		
		
		for(String publisher: publisher_list) {
			
			try {
				long au_count = loadNewBooksForAPublisher( publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_au_count += au_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":loadNewBooksFromPublisherListFile Error loading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		String root_newcontentdir = inputDir + File.separator + new_content_dir ;					// input/ebook/newcontent_202109
		
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewBooksFromPublisherListFile summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + pub_count + " have new content been exported");
		logger.info("Total exported " + total_au_count + " AUs to " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
	}

	
	
	
	private void loadNewBooksForAllPublishers() throws Exception {
		
		logger.info( programName + ":loadNewBooksForAllPublishers ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_au_count = 0;
		
		//all publisher list
		try {
			portico_tdm_book_publishers = TDMUtil.getTDMBookPublishers();
		} catch (Exception e) {
			logger.error( programName + ":loadNewBooksForAllPublishers Error getting publisher ids from tdm_publisher table. " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int portico_tdm_book_pub_count = portico_tdm_book_publishers.size();
		int new_content_pub_count = 0;
		
		for(String publisher: portico_tdm_book_publishers) {
			
			try {
				long au_count = loadNewBooksForAPublisher(publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_au_count += au_count;
				}
			} catch (Exception e) {
				logger.error( programName + ":loadNewBooksForAllPublishers Error loading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		String new_content_dir = getNewContentFolder();												//       ebook/newcontent_202109
		String root_newcontentdir = inputDir + File.separator + new_content_dir ;					// input/ebook/newcontent_202109
		
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewBooksForAllPublishers summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + portico_tdm_book_pub_count + " have new content been exported");
		logger.info("Total exported " + total_au_count + " AUs to " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
	}

	

	private void loadNewContentFromPublisherListFile(String publistfile) throws Exception {
		
		logger.info( programName + ":loadNewContentFromPublisherListFile ");
		
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
			logger.error( programName + ":loadNewContentFromPublisherListFile Error getting publisher ids from file " + publistfile + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int pub_count = publisher_list.size();
		int new_content_pub_count = 0;
		
		
		for(String publisher: publisher_list) {
			
			try {
				long au_count = loadNewContentForAPublisher(publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_au_count += au_count;
				}
				
			} catch (Exception e) {
				logger.error( programName + ":loadNewContentFromPublisherListFile Error loading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
        String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String root_newcontentdir = inputDir + File.separator + "newcontent_" + loadToDate.substring(6) + loadToDate.substring(0,2) ;
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewContentFromPublisherListFile summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + pub_count + " have new content been exported");
		logger.info("Total exported " + total_au_count + " AUs to " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
	}

	
	
	
	private void loadNewContentForAllPublishers() throws Exception {
		
		logger.info( programName + ":loadNewContentForAllPublishers ");
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		long total_au_count = 0;
		
		//all publisher list
		try {
			portico_tdm_journal_publishers = TDMUtil.getTDMJournalPublishers();
		} catch (Exception e) {
			logger.error( programName + ":loadNewContentForAllPublishers Error getting publisher ids from tdm_publisher table. " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		int portico_tdm_journal_pub_count = portico_tdm_journal_publishers.size();
		int new_content_pub_count = 0;
		
		for(String publisher: portico_tdm_journal_publishers) {
			
			try {
				long au_count = loadNewContentForAPublisher(publisher);
				
				if ( au_count > 0 ) {
					new_content_pub_count ++;
					total_au_count += au_count;
				}
			} catch (Exception e) {
				logger.error( programName + ":loadNewContentForAllPublishers Error loading publisher " + publisher + " " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//print stats list
		//for(String line: stats_list) {
		//	logger.info(line);
		//}
		
		//summary
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
        String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String root_newcontentdir = inputDir + File.separator + "newcontent_" + loadToDate.substring(6) + loadToDate.substring(0,2) ;
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewContentForAllPublishers summary:"  );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total " +  new_content_pub_count + " publishers out of " + portico_tdm_journal_pub_count + " have new content been exported");
		logger.info("Total exported " + total_au_count + " AUs to " + root_newcontentdir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
	}
	
	

	
	/**
	 * Create book AU list for a publisher in config/publisherID_book_list.
	 * The book AU ids are read from ana_unified_book, ana_unibook_holding table, which has de-duped book list.
	 * @param conn
	 * @param publisherID
	 * @param publisherBookCSName 
	 * @return config/led_book_list
	 * @throws IOException 
	 * @throws SQLException 
	 */
	private String createDedupedBookAUListForAContentSetFromANATable(Connection conn, String publisherID, String publisherBookCSName) throws IOException, SQLException {
		
		String fileName =  configDir + File.separator + publisherID.toLowerCase() + "_book_list";
		
		logger.info( programName + ":createBookAUListForAContentSetFromANATable creating book list for " + publisherID + " to " + fileName + " ...");
		
		List<String> book_list = null;
		try {
			book_list = TDMUtil.getDedupedBookListForAPublisher(conn, publisherID, publisherBookCSName);
		} catch (Exception e1) {
			logger.error( programName + ":createBookAUListForAContentSetFromANATable :getBookListForAPublisher" + publisherID + " " + publisherBookCSName + " "+ e1.getMessage());
			e1.printStackTrace();
		}

		//output to list_file_name
		Path filePath = Paths.get( fileName );
		 
		try {
			Files.write(filePath, book_list);
		} catch (IOException e) {
			logger.error( programName + ":createBookAUListForAContentSetFromANATable Error write au list to file " + fileName + " "+ e.getMessage());
			throw e;
		}
		
		return fileName;

	}

	
	/**
	 * Export newly changed AU of a content set in giving time range to input/newcontent_yyyyMM/publisher_id_lowercase/ folder
	 * @param cs
	 * @throws Exception 
	 */
/*	private void loadNewContentForAContentSet_1(String cs) throws Exception {
		
		logger.info( programName + ":loadNewContentForAContentSet " + cs );
		
		
		String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String loadFromDate = getLoadDateSince();
		
		
		String new_content_dir = getNewContentFolder();												//       newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/led
		
		
		ExportAu exportor = new ExportAu();
		
		exportor.setContentType(CONTENT_TYPE_JOURNAL);
		exportor.setInputType("ContentSet");
		exportor.setInputValue(cs);
		String proj_dir = System.getProperty("user.dir");
		exportor.setDestFolder("file:" + proj_dir + File.separator + newContentInputDir );
		exportor.setIngestDateFrom("NA");
		exportor.setIngestDateTo("NA");
		exportor.setAuContentModifiedDateFrom(loadFromDate);
		exportor.setAuContentModifiedDateTo(loadToDate);
		
		exportor.exportJournalAUForACS();
		
		
		
	}*/
	
	/**
	 * Export newly changed AU of a content set in giving time range to input/newcontent_yyyyMM/publisher_id_lowercase/ folder
	 * @param cs
	 * @throws Exception 
	 */
	private void loadNewContentForAContentSet(String cs) throws Exception  {
		
		logger.info( programName + ":loadNewContentForAContentSet " + cs );
		
		
		String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String loadFromDate = null;
		
		if ( isSinceBeginningFlag()) {
			try {
				loadFromDate = TDMUtil.findContentSetStartDate(cs);
			}
			catch(Exception e) {
				logger.error(programName + ": loadNewContentForAContentSet :Error getting content set journal start date " +  e.getMessage());
				loadFromDate = "01/01/1900";	//default date
			}
		}
		else {
			loadFromDate = getLoadDateSince();
		}
		
		ExportAu exportor = new ExportAu();
		exportor.setLoadDateSince(loadFromDate);
		exportor.setLoadDateTo(loadToDate);
		exportor.setAuContentModifiedDateFrom(loadFromDate);
		exportor.setAuContentModifiedDateTo(loadToDate);
		
		try {
			exportor.loadContentForAContentSet(cs);
		} catch (Exception e) {
			logger.error( programName + ":loadNewContentForAContentSet " + cs + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		
		
	}

	
	/**
	 * Export newly changed AUs of a publisher in giving time range to input/newcontent_yyyyMM/publisher_id_lowercase/ folder.
	 * @param publisher
	 * @return The AU count of exported content
	 * @throws Exception
	 */
	private long loadNewContentForAPublisher(String publisher) throws Exception {
		
		logger.info( programName + ":loadNewContentForAPublisher " + publisher );
		

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		String startTimeStr = dateFormat.format(cal.getTime());
		long start_time = System.currentTimeMillis();
		
		String loadToDate = getLoadDateTo();		//ie 03/02/2021
		String loadFromDate = null;
		
		if ( isSinceBeginningFlag() ) {
			try {
				loadFromDate = TDMUtil.findPublisherJournalBeginningDate(publisher);
			}
			catch(Exception e) {
				logger.error(programName + ": loadNewContentForAPublisher :Error getting publisher min journal start date " +  e.getMessage());
				loadFromDate = "01/01/1900";	//default date
			}
		}
		else {
			loadFromDate = getLoadDateSince();
		}
		
		String new_content_dir = getNewContentFolder();												//       newcontent_202109
		
		String subdir = new_content_dir + File.separator + publisher.toLowerCase();					//       newContent_202109/led
		String newContentInputDir = inputDir + File.separator + subdir;							// input/newContent_202109/led
		
		
		ExportAu exportor = new ExportAu();
		
		exportor.setLoadDateSince(loadFromDate);
		exportor.setLoadDateTo(loadToDate);
		
		String proj_dir = System.getProperty("user.dir");
		exportor.setDestFolder("file:" + proj_dir + File.separator + newContentInputDir );

		
		long au_count = 0;
		try {
			au_count = exportor.loadContentForAPublisher(publisher);
		}
		catch(Exception e) {
			logger.error(programName + ":loadNewContentForAPublisher :exportJournalAUFromCSList   " + publisher + " " + e.getMessage());
			e.printStackTrace();
		}
		

		
		cal = Calendar.getInstance(  );
		String endTimeStr = dateFormat.format(cal.getTime());
		long end_time = System.currentTimeMillis();
		long totaltime = end_time -start_time;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
		
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info(programName + ":loadNewContentForAPublisher " + publisher );
		logger.info("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		logger.info("Total exported " + au_count + " AUs to " + newContentInputDir );
		logger.info("-------------------------------------------------------------------------------------------");
		logger.info("-------------------------------------------------------------------------------------------");
		
			
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add(programName + ":loadNewContentForAPublisher " + publisher );
		stats_list.add("Start at " + startTimeStr + ", end at " + endTimeStr + ". Total running time=" + timeUsedStr);
		stats_list.add("Total exported " + au_count + " AUs to " + newContentInputDir );
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("-------------------------------------------------------------------------------------------");
		stats_list.add("\n");
		
		
		return au_count;
	}
	
	
	/**
	 * This method scans a new content dir and returns the AU counts in the dir.
	 * The directory structure is input/newcontent_2021MM/publisher/CS/AU/.
	 * @param publisher
	 * @param newcontentdir input/newcontent_202109/led
	 * @return
	 */
	private long scanDirForAuCount(String publisher, String newcontentdir) {
		
		int minDepth = 2;
		int maxDepth = 2;
		Path rootPath = Paths.get(newcontentdir);
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
			logger.error( programName + ":scanDirForAuCount " + newcontentdir + " " + e.getMessage());
			e.printStackTrace();
		}
	

		
		return count;
	}
	




	private static void printUsage(Options options) {
		
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "TDMPorticoJournalNewContentLoader", options);  
		writer.flush();  
		
	}

	private void printStats() {
		
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

        logger.info("Loading time " + startTimeStr + "[start] to " + endTimeStr + "[end], in total = " + timeUsedStr );
		
	}

	private static Options constructOptions() {
		final Options options = new Options();

		options.addOption(Option.builder("cs").required(false).hasArg().desc("Create json file for all AUs for a content set").build() );
		options.addOption(Option.builder("csfile").required(false).hasArg().desc("Create json file for all content sets in a file, one per line").build() );
		options.addOption(Option.builder("publisher").required(false).hasArg().desc("Create json file for a publisher").build() );
		options.addOption(Option.builder("publist").required(false).hasArg().desc("Create json file for publishers in the file").build() );
		
		options.addOption(Option.builder("loadnew").required(false).desc("Load modifiled AUs to input/newcontent_yyyyMM").build() );
		options.addOption(Option.builder("process").required(false).desc("Process new content from input/newcontent_yyyyMM").build() );
		options.addOption(Option.builder("upload").required(false).desc("Upload processed new content from output/newcontent_yyyyMM to AWS s3").build() );
		options.addOption(Option.builder("auto").required(false).desc("Auto export/process/cache/upload new content from since date to loadto date").build() );
		
		options.addOption(Option.builder("book").required(false).desc("The content be processed is book type").build() );
		
		
		options.addOption(Option.builder("since").required(false).hasArg().desc("Load modifiled AUs since this date, in format of MM/dd/yyyy.  Default is last month first day").build() );
		options.addOption(Option.builder("to").required(false).hasArg().desc("Load modifiled AUs to this date, in format of MM/dd/yyyy.  Default is this month first day").build() );


		
		return options;
	}

	public String getSubDir() {
		return subDir;
	}

	public void setSubDir(String subDir) {
		this.subDir = subDir;
	}

	public String getArgStr() {
		return argStr;
	}

	public void setArgStr(String argStr) {
		this.argStr = argStr;
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

	public List<String> getUploadedJsonlineFileList() {
		return uploadedJsonlineFileList;
	}

	public void setUploadedJsonlineFileList(List<String> uploadedJsonlineFileList) {
		this.uploadedJsonlineFileList = uploadedJsonlineFileList;
	}

	public boolean isSinceBeginningFlag() {
		return sinceBeginningFlag;
	}

	public void setSinceBeginningFlag(boolean sinceBeginningFlag) {
		this.sinceBeginningFlag = sinceBeginningFlag;
	}



}


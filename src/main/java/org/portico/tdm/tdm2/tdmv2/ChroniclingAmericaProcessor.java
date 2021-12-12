package org.portico.tdm.tdm2.tdmv2;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.portico.tdm.tdm2.tools.TDMUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * This class is used for processing and uploading Chronicling America data to tdm pilot project.
 * Usage:
 * (linux run '. setenv.sh' first)
 * 	java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -titlelist cslist.txt -upload
 * 	java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -issueidlist chronam_miss_issue.txt -local -upload -combineby 500
 * 	java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -issueid https://chroniclingamerica.loc.gov/lccn/sn85058012/1897-09-11/ed-1 -upload
 *  java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -lccn sn85038615  -upload  (4000 issues) 
 *  java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -lccn sn84020639  -upload (331 issues)
 *  java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -lccn sn85038615  -issue 1903-01-27 -upload
 *  java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -lccn sn86091254  -issue 1959-11-01 -local
 *  java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -lccn sn86091254 -checknew -local
 *  java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -lccn sn86091254 -local -upload
 *  java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -subdir chronam -issueidlist chronam_issueid_ad -loadonly -combineby 400
 *  java org.portico.tdm.tdmv2.ChroniclingAmericaProcessor -addbatch 
 * @author dxie
 *
 */
public class ChroniclingAmericaProcessor {
	
	static Properties props = new Properties();
	static Logger logger = LogManager.getLogger(ChroniclingAmericaProcessor.class.getName());
	static String programName = "ChroniclingAmericaProcessor";
	static String collectionId = "Chronicling America";
	
	String outputDir = "output";
	String inputDir = "input";
	String subDir = "";
	String server = "PROD";
	boolean bookType = false;
	boolean journalType = true;
	
	long starttime, endtime;
	String startTimeStr, endTimeStr;
	String argStr;

	List<String> titleProcessed;
	List<String> titleFailed;
	
	List<String> jsonFileCreated;
	List<String> jsonFileNotCreated;
	
	List<String> jsonLineFileUploaded;
	List<String> jsonLineFileFailedUpload;
	
	List<String> missingFileList;
	
	boolean uploadFlag = false;
	int combineBySize = 300;		//default size
	
	boolean localFlag = false;		//set to true if use local ocr.txt for issue full text
	boolean checkNewFlag = false;	//set to true if do not process existed issue in tdm_newspaper
	boolean loadOnlyFlag = false;	//set to true if only upload json file, not creating issue json file
	
	public ChroniclingAmericaProcessor() {
		
		//get startTime
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		startTimeStr = dateFormat.format(cal.getTime());
		starttime = System.currentTimeMillis();
		
		titleProcessed = new ArrayList<>();
		titleFailed = new ArrayList<>();
		
		jsonFileCreated = new ArrayList<>();
		jsonFileNotCreated = new ArrayList<>();
		
		jsonLineFileUploaded = new ArrayList<>();
		jsonLineFileFailedUpload = new ArrayList<>();
		
		missingFileList = new ArrayList<>();
	}
	
	
	public static void main(String[] args) {
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();

		ChroniclingAmericaProcessor processor = new ChroniclingAmericaProcessor();
		try {
			CommandLine line = parser.parse( options, args );
			
			logger.info( programName + " with args: " + String.join(" ", args));
			processor.setArgStr( String.join(" ", args)) ;

			if ( line.hasOption("subdir")) {		//required for all input
				processor.setSubDir(line.getOptionValue("subdir"));
			}
			else if ( ! line.hasOption("addbatch")){
				ChroniclingAmericaProcessor.printUsage( options );
				System.exit(-1);
			}
			
			if ( line.hasOption("local")) {
				processor.setLocalFlag(true);
			}
			else {
				processor.setLocalFlag(false);
			}
			
			if ( line.hasOption("checknew")) {
				processor.setCheckNewFlag(true);
			}
			else {
				processor.setCheckNewFlag(false);
			}
			
			if ( line.hasOption("loadonly")) {
				processor.setLoadOnlyFlag(true);
				
				if ( line.hasOption("combineby")) {
					String size =  line.getOptionValue("combineby");
					if ( size.matches( "\\d+") ) {
						processor.setCombineBySize(Integer.parseInt(size));
					}
				}
				else {
					processor.setCombineBySize(300);		//default size
				}
			}
			else {
				processor.setLoadOnlyFlag(false);
			}
			
			if ( line.hasOption("upload") ) {
				processor.setUploadFlag(true);
				
				if ( line.hasOption("combineby")) {
					String size =  line.getOptionValue("combineby");
					if ( size.matches( "\\d+") ) {
						processor.setCombineBySize(Integer.parseInt(size));
					}
				}
				else {
					processor.setCombineBySize(300);		//default size
				}
			}
			
			if ( line.hasOption("lccn")  ) {
				String lccn = line.getOptionValue("lccn");
				
				if ( line.hasOption("issue")) {
					String issue_date = line.getOptionValue("issue");
					
					if ( issue_date.matches("^\\d{4}-\\d{2}-\\d{2}")) {
						try {
							processor.processOneIssue(lccn, issue_date);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else {
						ChroniclingAmericaProcessor.printUsage(options);
					}
				}
				else {
					
					try {
						processor.processOneTitle(lccn);
						
					} catch (Exception e) {
					
						e.printStackTrace();
					}

				}
			}
			else if ( line.hasOption("issueid")) {
				String issueid = line.getOptionValue("issueid");
				
				try {
					processor.processOneIssue(issueid);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else if ( line.hasOption("titlelist")) {
				String titlefile = line.getOptionValue("titlelist");
				
				try {
					processor.processTitleList(titlefile);
				} catch (Exception e) {
					
					e.printStackTrace();
				}
			}
			else if ( line.hasOption("issueidlist")) {
				String listfile = line.getOptionValue("issueidlist");
				
				try {
					processor.processIssueIdList(listfile);
				} catch (Exception e) {
					
					e.printStackTrace();
				}
			}
			else if ( line.hasOption("addbatch")) {
				try {
					processor.processBatchJson("https://chroniclingamerica.loc.gov/batches.json");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			processor.printSummary();
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}




	/**
	 * Go through all batch files, starting from https://chroniclingamerica.loc.gov/batches.json
	 * and populate tdm_batch table. 
	 * @throws Exception 
	 */
	private void processBatchJson( String batch_url ) throws Exception {
		
		JSONObject jo = null;
		try {
			jo = TDMUtil.getJsonObjectFromUrl(batch_url);
		} catch (Exception e) {
			logger.error( programName + ":processBatch :getJsonObjectFromUrl " + batch_url + " " + e );
			throw e;
		}
		
		JSONArray batches = ((JSONArray)jo.get("batches")); 
        Iterator itr1 = batches.iterator(); 
                
        while (itr1.hasNext()){
        	JSONObject batch_json = (JSONObject) itr1.next();
        	
        	TDMCABatch batch = new TDMCABatch(batch_json);

        	batch.checkLccns();
        	
        }
		
		
		String next_batch = (String) jo.get("next");
		
		if ( next_batch!= null ) {
			processBatchJson(next_batch);
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
        logger.info("Process summary: " );
        logger.info("Title processed =" + titleProcessed.size());
        for( String title: titleProcessed ) {
        	logger.info( "\t" + title);
        }
        logger.info("Title failed = " + titleFailed.size() ) ;
        for( String title: titleFailed ) {
        	logger.info( "\t" + title);
        }
        logger.info("Issue json file created =" + jsonFileCreated.size());
        for( String title: jsonFileCreated ) {
        	logger.info( "\t" + title);
        }
        logger.info("Issue json file not created = " + jsonFileNotCreated.size() ) ;
        for( String title: jsonFileNotCreated ) {
        	logger.info( "\t" + title);
        }
        
        if ( isUploadFlag() || isLoadOnlyFlag() ) {
        	logger.info("Successfully uploaded json line files (" + jsonLineFileUploaded.size() + "): " );
        	for( String jsonlinefile: jsonLineFileUploaded ) {
        		logger.info( "\t" + jsonlinefile);
        	}
        	
        	logger.info("Failed upload json line files (" + jsonLineFileFailedUpload.size() + "): " );
        	for( String jsonlinefile: jsonLineFileFailedUpload ) {
        		logger.info( "\t" + jsonlinefile);
        	}
        	
        	logger.info("Missing json files (" + missingFileList.size() + "): " );

        	for( String missingjsonfile: missingFileList ) {
        		logger.info( "\t" + missingjsonfile);
        	}
        }
        
        logger.info("Total run time " + startTimeStr + "[start] to " + endTimeStr + "[end], in total = " + timeUsedStr );
        logger.info("================================================================");
		
	}


	/**
	 * Parse titlelist file and process all titles in the file
	 * @param titlefile ie one lccn per line
	 * @throws Exception 
	 */
	private void processTitleList(String titlefile) throws Exception {

		List<String> lccns = null;
		
		try {
			lccns = TDMUtil.getListFromFile(titlefile);
		}
		catch(Exception e) {
			logger.error( programName + ":processTitleList :getListFromFile " + titlefile + ". " + e.getMessage());
			throw new Exception("Error get lccn list");
		}

		for(int i =0; i< lccns.size(); i++) {
			String lccn = lccns.get(i);
			try {
				logger.info( programName + ":processTitleList Process title " + (i+1) + " " + lccn);
				processOneTitle(  lccn );

			}
			catch(Exception e) {
				logger.error(programName + ":processTitleList :processOneTitle " + lccn + ". " + e.getMessage());

			}
		}

	}
	
	

	/**
	 * This method process a list of issue ids, ie 'https://chroniclingamerica.loc.gov/lccn/sn85058012/1897-09-11/ed-1'
	 * @param listfile
	 * @throws Exception 
	 */
	private void processIssueIdList(String listfile) throws Exception {
		List<String> issueIdList = null;
		List<String> issueJsonFileList = new ArrayList<>();
		
		try {
			issueIdList = TDMUtil.getListFromFile(listfile);
		}
		catch(Exception e) {
			logger.error( programName + ":processIssueIdList :getListFromFile " + listfile + ". " + e.getMessage());
			throw new Exception("Error get issue id list");
		}
		
		String subdir = getSubDir();
		boolean local = isLocalFlag();
		boolean loadonlyflag = isLoadOnlyFlag();
		
		for(int i =0; i< issueIdList.size(); i++) {
			String issueid = issueIdList.get(i);			//https://chroniclingamerica.loc.gov/lccn/sn83030214/1877-12-24/ed-1

			logger.info( programName + ": " + (i+1) + ", process issue " + issueid );


			Pattern p = Pattern.compile("https://chroniclingamerica.loc.gov/lccn/([^/]*)/(\\d{4}-\\d{2}-\\d{2})/ed-(.*)");
			Matcher m = p.matcher(issueid);
			
			String lccn = null;
			String issue_date = null;
			String edition = null;
			
			if ( m.find()) {
				lccn = m.group(1);
				issue_date = m.group(2);
				edition = m.group(3);
			}

			V2Newspaper newspaper = new V2Newspaper(collectionId, lccn, subdir );

			try {
				newspaper.populateMetadata();
			} catch (Exception e) {
				logger.error( programName + ":processOneIssue :populateMetadata " + lccn + " " + e);
				e.printStackTrace();
			}

			titleProcessed.add(lccn);

			String jsonFile = null;
			if ( loadonlyflag ) {
				
				jsonFile = "output" + File.separator + "chronam" + File.separator + lccn + File.separator + lccn + "_" + issue_date + "_ed-" + edition + ".json"; 
				
				if ( ! Files.exists(Paths.get(jsonFile)) ) {
					logger.error( programName + ":processOneIssue: json file does not exist " + jsonFile );
					throw new Exception("Json file not exist");
				}
				
				issueJsonFileList.add(jsonFile);
				
			}
			else {
				try {
					jsonFile = newspaper.processOneIssue(local, issue_date);

					jsonFileCreated.add(jsonFile);
					issueJsonFileList.add(jsonFile);

				} catch (Exception e) {
					logger.error( programName + ":processIssueIdList :processOneIssue " + issueid + " " + e);
					jsonFileNotCreated.addAll(newspaper.getIssueJsonFailed());
					throw e;
				}
			}
			
		}
		
		if ( isUploadFlag() || loadonlyflag ) {
			try {
				uploadIssues( getCombineBySize(), issueJsonFileList );
				
			} catch (IOException e) {
				logger.error( programName + ":processIssueIdList :uploadIssues " + e);
				throw e;
			}
		}
		

		
		
	}


	private void uploadIssues(int combinesize, List<String> jsonFileList ) throws Exception {
		
	
		if (jsonFileList == null || jsonFileList.isEmpty()) {
			logger.info( programName + ":uploadIssues: Empty json file list " );
			throw new Exception("Empty json file list");
		}
		
		
		int groupCount = 0;
		if ( jsonFileList.size() % combinesize == 0 ) {
			groupCount = jsonFileList.size()/combinesize;
		}
		else {
			groupCount = jsonFileList.size()/combinesize + 1;
		}
		
		int startIndex = 0;
		int endIndex = combinesize;
		Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		String tsStr = dateFormat.format(snapshotDate);

		
		for( int i=0; i< groupCount; i ++ ) {
			
			if (endIndex > jsonFileList.size()) {
				endIndex = jsonFileList.size();
			}
			

			//get a sub list of jsonFileList
			List<String> sliceList = null;
			
			try {
				sliceList = jsonFileList.subList(startIndex, endIndex);
			}
			catch(IndexOutOfBoundsException e) {
				logger.error( programName + ":uploadIssues :Error get sub list of json file list from " + startIndex + " to " + endIndex  );
				
				startIndex += combinesize;
				endIndex += combinesize;
				if (endIndex > jsonFileList.size()) {
					endIndex = jsonFileList.size();
				}

			}

			String jsonlinefilename = "issuelist_" + tsStr + "_" + i + ".jsonl";
			String jsonlinefileWithPath = outputDir + File.separator + subDir + File.separator + jsonlinefilename; 
			
			try {
				List<String> missingJsonFiles = TDMUtil.concatenateJsonFilesToOneFile( sliceList, jsonlinefileWithPath );
				jsonFileNotCreated.addAll(missingJsonFiles);
	
				jsonLineFileUploaded.add(jsonlinefileWithPath);

				
			} catch (Exception e) {
				logger.error( programName + ":uploadIssues " + e );
				e.printStackTrace();
				startIndex += combinesize;
				endIndex += combinesize;
				continue;		//any group failed, continue to next group
				
			}
			
			startIndex += combinesize;
			endIndex += combinesize;

		}
		
		//Gzip jsonlinefiles. gzipJsonLineFile has path 
		List<String> gzipJsonLineFile = TDMUtil.gzipFiles( jsonLineFileUploaded );  

		//upload gzipped jsonline files
		TDMUtil.uploadFiles( gzipJsonLineFile );
		

		//log upload timestamp to TDM_NEWSPAPER table
		try   {
			TDMUtil.logNewspaperIssueUploadTimeToDB( jsonFileList );
		}
		catch(Exception e) {
			logger.error( programName + ":uploadIssues " + e.getMessage());
		}

		
	}


	/**
	 * Process one newspaper title. localFlag, checkNewFlag, loadOnlyFlag have effect on this method.
	 * @param lccn
	 * @return
	 */
	private V2Newspaper processOneTitle(String lccn) {
		
		String subdir = getSubDir();
		
		V2Newspaper newspaper = new V2Newspaper(collectionId, lccn, subdir );
		
		try {
			newspaper.populateMetadata();
		} catch (Exception e) {
			logger.error( programName + ":processOneTitle :populateMetadata " + lccn + " " + e);
			e.printStackTrace();
		}
		
		boolean checkflag = isCheckNewFlag();
		
		try {
			newspaper.createIssues(checkflag);
		} catch (Exception e) {
			logger.error( programName + ":processOneTitle :createIssues " + lccn + " " + e);
			e.printStackTrace();
		}
		
		boolean loadonlyflag = isLoadOnlyFlag();
		List<String> jsonFileList = null;
		
		if ( loadonlyflag ) {					//skip creating issue json files, upload issues queried from table
			try {
				jsonFileList = newspaper.uploadTableIssues( getCombineBySize() );
			} catch (Exception e) {
				logger.error( programName + ":processOneTitle :uploadIssues (loadonly ) " + lccn + " " + e);
				jsonLineFileFailedUpload.addAll(newspaper.getJsonLineFilesFailed());
				e.printStackTrace();
			}
		}
		else {

			boolean local = isLocalFlag();
			boolean upload = isUploadFlag();

			try {
				jsonFileList = newspaper.processIssues(local, checkflag );		//create issue json files, either from local or from url
				if ( jsonFileList != null ) {
					jsonFileCreated.addAll(jsonFileList);
				}

			} catch (Exception e) {
				logger.error( programName + ":processOneTitle :processIssues " + lccn + " " + e);
				jsonFileNotCreated.addAll(newspaper.getIssueJsonFailed());
				e.printStackTrace();
			}

			if ( upload ) {
				try {
					newspaper.uploadIssues( getCombineBySize(), jsonFileList );
					jsonLineFileUploaded.addAll(newspaper.getJsonLineFilesUploaded());
				} catch (Exception e) {
					logger.error( programName + ":processOneTitle :uploadIssues " + e);
					jsonLineFileFailedUpload.addAll(newspaper.getJsonLineFilesFailed());
					e.printStackTrace();
				}
			}
		}
		
		titleProcessed.add(lccn);
		
		return newspaper;
		
	}


	/**
	 * Process one newspaper issue. localFlag and loadOnlyFlag have effect on this method. checkNewFlag won't be checked.
	 * @param issueid
	 * @throws Exception 
	 */
	private void processOneIssue(String issueid) throws Exception {
		
		String lccn = getLccnFromIssueId(issueid);
		String issue_date = getIssueDateFromIssueId(issueid);
		
		processOneIssue( lccn, issue_date );

	}

	/**
	 * Retrieve issue date from issue id 
	 * @param issueid https://chroniclingamerica.loc.gov/lccn/sn85058012/1897-09-11/ed-1
	 * @return 1897-09-11
	 */
	private String getIssueDateFromIssueId(String issueid) {
		String issue_date_str = null;
		
		Pattern p = Pattern.compile("https://chroniclingamerica.loc.gov/lccn/[^/]*/(\\d{4}-\\d{2}-\\d{2})/.*");
		Matcher m = p.matcher(issueid);
		
		if ( m.find()) {
			issue_date_str = m.group(1);
		}
		
		return issue_date_str;
		
	}


	/**
	 * Retrieve lccn from issue id 
	 * @param issueid https://chroniclingamerica.loc.gov/lccn/sn85058012/1897-09-11/ed-1
	 * @return sn85058012
	 */
	private String getLccnFromIssueId(String issueid) {
		String lccn = null;
		
		Pattern p = Pattern.compile("https://chroniclingamerica.loc.gov/lccn/([^/]*)/.*");
		Matcher m = p.matcher(issueid);
		
		if ( m.find()) {
			lccn = m.group(1);
		}
		
		return lccn;
		
	}


	/**
	 * Process one newspaper issue. localFlag and loadOnlyFlag have effect on this method. checkNewFlag won't be checked.
	 * @param lccn
	 * @param issue_date
	 * @throws Exception 
	 */
	private void processOneIssue(String lccn, String issue_date) throws Exception {
		String subdir = getSubDir();
		boolean local = isLocalFlag();
		boolean loadonlyflag = isLoadOnlyFlag();
		
		V2Newspaper newspaper = new V2Newspaper(collectionId, lccn, subdir );
		
		try {
			newspaper.populateMetadata();
		} catch (Exception e) {
			logger.error( programName + ":processOneIssue :populateMetadata " + lccn + " " + e);
			e.printStackTrace();
		}
		
		titleProcessed.add(lccn);
		
		String jsonFile = null;
		
		if( loadonlyflag) {		//do not create json file, use json file already been created
			jsonFile = "output" + File.separator + "chronam" + File.separator + lccn + File.separator + lccn + "_" + issue_date + "_ed-1.json"; 
			
			if ( ! Files.exists(Paths.get(jsonFile)) ) {
				logger.error( programName + ":processOneIssue: json file does not exist " + jsonFile );
				throw new Exception("Json file not exist");
			}
			
		}
		else {

			try {
				jsonFile = newspaper.processOneIssue(local, issue_date);

				jsonFileCreated.add(jsonFile);

			} catch (Exception e) {
				logger.error( programName + ":processOneIssue :processIssues " + lccn + " " + e);
				jsonFileNotCreated.addAll(newspaper.getIssueJsonFailed());
				e.printStackTrace();
			}
		}
		
		if ( isUploadFlag() || loadonlyflag ) {
			try {
				String jsonlinefile = newspaper.uploadOneIssue( jsonFile );
				jsonLineFileUploaded.add(jsonlinefile);
			} catch (IOException e) {
				logger.error( programName + ":processOneIssue :uploadOneIssue " + lccn + " " + e);
				jsonLineFileFailedUpload.addAll(newspaper.getJsonLineFilesFailed());
				e.printStackTrace();
			}
		}
		
		
		
		
	}


	private static void printUsage(Options options) {
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "ChroniclingAmericaProcessor", options);  
		writer.flush();  
		
	}


	private static Options constructOptions() {
		final Options options = new Options();

		options.addOption(Option.builder("titlelist").required(false).hasArg().desc("Newspaper title list").build() );
		options.addOption(Option.builder("issueidlist").required(false).hasArg().desc("Newspaper issue id list").build() );
		options.addOption(Option.builder("issueid").required(false).hasArg().desc("Newspaper issue id, ie https://chroniclingamerica.loc.gov/lccn/sn85058012/1897-09-11/ed-1").build() );
		options.addOption(Option.builder("lccn").required(false).hasArg().desc("One newspaper to process").build() );
		options.addOption(Option.builder("issue").required(false).hasArg().desc("Use with lccn, one issue of a newspaper to process. Format yyyy-MM-dd").build() );
		options.addOption(Option.builder("subdir").required(false).hasArg().desc("The subdirectory name under input directory where AU files of Content Set have been stored.").build());
		options.addOption(Option.builder("upload").required(false).desc("upload to S3").build());
		options.addOption(Option.builder("combineby").required(false).hasArg().desc("How many json files are combined together").build());
		options.addOption(Option.builder("local").required(false).desc("Read ocr file from local drive, instead of fetching from url").build());
		options.addOption(Option.builder("checknew").required(false).desc("Check first if the issue has been processed before. Skip processing if yes. ").build());
		options.addOption(Option.builder("loadonly").required(false).desc("Only upload").build());
		options.addOption(Option.builder("addbatch").required(false).desc("Go through batch json files and populate tdm_batch table").build());
		
		return options;
	}


	public String getInputDir() {
		return inputDir;
	}


	public void setInputDir(String inputDir) {
		this.inputDir = inputDir;
	}


	public String getSubDir() {
		return subDir;
	}


	public void setSubDir(String subDir) {
		this.subDir = subDir;
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


	public boolean isUploadFlag() {
		return uploadFlag;
	}


	public void setUploadFlag(boolean uploadFlag) {
		this.uploadFlag = uploadFlag;
	}


	public int getCombineBySize() {
		return combineBySize;
	}


	public void setCombineBySize(int combineBySize) {
		this.combineBySize = combineBySize;
	}


	public static String getCollectionId() {
		return collectionId;
	}


	public static void setCollectionId(String collectionId) {
		ChroniclingAmericaProcessor.collectionId = collectionId;
	}


	public boolean isLocalFlag() {
		return localFlag;
	}


	public void setLocalFlag(boolean localFlag) {
		this.localFlag = localFlag;
	}


	public boolean isCheckNewFlag() {
		return checkNewFlag;
	}


	public void setCheckNewFlag(boolean checkNewFlag) {
		this.checkNewFlag = checkNewFlag;
	}


	public boolean isLoadOnlyFlag() {
		return loadOnlyFlag;
	}


	public void setLoadOnlyFlag(boolean loadOnlyFlag) {
		this.loadOnlyFlag = loadOnlyFlag;
	}

}

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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.tools.TDMUtil;

/**
 * This class process 4 Docsouth collections data.
 * It uses downloaded xml file to retrieve metadata, convert to text file with page breaks, and perform TDM tasks.
 * @author dxie
 *
 */
public class V2DocSouthProcessor {
	
	
	static Properties props = new Properties();
	static Logger logger = LogManager.getLogger(V2DocSouthProcessor.class.getName());
	static String programName = "V2DocSouthProcessor";
	static String collectionId = "DocSouth";
	
	String outputDir = "output";
	String inputDir = "input";
	String subDir = "";						//docsouth
	String server = "PROD";
	
	String docsouth_collection_id;			//church, southlit, neh, fpn
	boolean uploadFlag = false;

	List<String> processedTitles;
	List<String> createdJsonFiles;
	List<String> createdJsonLineFiles;
	List<String> failedTitles;
	List<String> failedJsonFiles;
	List<String> uploadedJsonFiles;
	
	long starttime, endtime;
	String startTimeStr, endTimeStr;
	String argStr;
	
	
	public V2DocSouthProcessor() {
		//get startTime
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		startTimeStr = dateFormat.format(cal.getTime());
		starttime = System.currentTimeMillis();
		
		processedTitles = new ArrayList<>();
		createdJsonFiles = new ArrayList<>();
		createdJsonLineFiles = new ArrayList<>();
		failedTitles = new ArrayList<>();
		failedJsonFiles = new ArrayList<>();
	}

	public static void main(String[] args) {
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();

		V2DocSouthProcessor processor = new V2DocSouthProcessor();
		try {
			CommandLine line = parser.parse( options, args );
			
			logger.info( programName + " with args: " + String.join(" ", args));
			processor.setArgStr( String.join(" ", args)) ;

			if ( line.hasOption("subdir")) {		//required for all input
				processor.setSubDir(line.getOptionValue("subdir"));
			}
			
			if ( line.hasOption("upload")) {
				processor.setUploadFlag(true);
			}
			else {
				processor.setUploadFlag(false);
			}

			if ( line.hasOption("collection")) {
				String collection = line.getOptionValue("collection");
				processor.setDocsouth_collection_id(collection);
				
				if ( line.hasOption("xml")) {
					String xmlfile = line.getOptionValue("xml");		//"church-negrochurch-dubois.xml"
					processor.processOneTitle(collection, xmlfile);
				}
				else {
					processor.processOneCollection(collection);
				}

			}

			processor.printSummary();
			
		} catch (ParseException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	

	}
	
	
	/**
	 * 
	 * @param collectionid
	 * @param xmlfile
	 * @throws Exception 
	 */
	private void processOneTitle(String docsouth_coll_id, String xmlfile) throws Exception {
		
		String jsonfile = null;
		try {
			jsonfile = processOneDocSouthXmlFile(docsouth_coll_id, xmlfile );
		} catch (Exception e) {
			logger.error( programName + ":processOneCollection: processOneDocSouthXmlFile: " + docsouth_coll_id + " " + xmlfile + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		boolean uploadFlag = isUploadFlag();
		if ( uploadFlag ) {
			String jsonlinefilename = xmlfile.replace("xml", "jsonl");
			String jsonlineFileWithPathName =  getOutputDir() + File.separator + getSubDir() + File.separator + jsonlinefilename;
					
			Path jsonlineFilePath = Paths.get(  jsonlineFileWithPathName );
			Path jsonFilePath = Paths.get(jsonfile);

		    try {
		    	Files.copy( jsonFilePath, jsonlineFilePath); 
		    } catch (IOException e) {
		    	logger.error( programName + ":uploadOneCollection : Error concatenate to " + jsonlineFileWithPathName + " " + e.getMessage());
		    	throw new Exception("Error appending to file");
		    }
			
			//Gzip jsonlinefiles. gzipJsonLineFile has path 
			String gzipJsonLineFile = TDMUtil.gzipFile( jsonlineFileWithPathName );  

			//upload gzipped jsonline files
			TDMUtil.UploadGzipJsonLineFile( gzipJsonLineFile );
			
			createdJsonLineFiles.add(jsonlineFileWithPathName);

		}
		
	}
	
	

	/**
	 * Iterate input/docsouth/[collection]/ directory, process /xml files and /text files
	 * @param collectionId
	 * @throws IOException 
	 */
	private void processOneCollection(String collectionId) throws IOException {
		
		String collXmlPath = inputDir + File.separator + "docsouth" + File.separator + collectionId + File.separator + "data" + File.separator + "xml";
		//get xml file list
		List<String> xmlfiles = null;
		
		try {
			xmlfiles = Files.list(Paths.get(collXmlPath))						//only this directory, not recursively
				.filter( path -> path.toString().endsWith(".xml") )
				.map( path->path.getFileName().toString())
				.collect(Collectors.toList());
		} catch (IOException e) {
			logger.error( programName + ":processOneCollection " + collectionId + e.getMessage());
			throw e;
		}
		
		List<String> jsonFiles = new ArrayList<>();
		
		for(String xmlfilename: xmlfiles) {
			//process each title
			try {
				String jsonfile = processOneDocSouthXmlFile(collectionId, xmlfilename );
				
				jsonFiles.add(jsonfile);
			} catch (Exception e) {
				logger.error( programName + ":processOneCollection: processOneDocSouthXmlFile: " + collectionId + " " + xmlfilename + e.getMessage());
				e.printStackTrace();
			}
		}
		
		//create .jsonl file and upload to S3
		boolean uploadFlag = isUploadFlag();
		if ( uploadFlag ) {
			try {
				uploadOneCollection(collectionId, jsonFiles);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	
	private void uploadOneCollection(String docsouth_coll_id, List<String> jsonFiles) throws Exception {
		
		//concatenate all json files of the collection into one json line file
		Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		String tsStr = dateFormat.format(snapshotDate);
		
		
		String jsonlinefilename = docsouth_coll_id + "_" + tsStr + ".jsonl";
		String jsonlineFileWithPathName =  getOutputDir() + File.separator + getSubDir() + File.separator + jsonlinefilename;
				
		Path jsonlineFilePath = Paths.get(  jsonlineFileWithPathName );
		// Charset for read and write
	    Charset charset = StandardCharsets.UTF_8;

	    // Join files (lines)
	    for (String  jsonfileWithPath : jsonFiles) {
	    	
	    	Path path = Paths.get( jsonfileWithPath );
	    	if ( ! Files.exists( path )) {
	    		logger.error( programName + ":uploadOneCollection : File not exist " + jsonfileWithPath);
	    		//missingFileList.add(jsonfileWithPath );
	    		continue;
	    	}
	    	
	        List<String> lines = null;
			try {
				lines = Files.readAllLines(path, charset);
			} catch (IOException e1) {
				logger.error( programName + ":uploadOneCollection : Error read lines from " + jsonfileWithPath + " " + e1.getMessage());
				throw new Exception("Error read json file");
			}
			
	        try {
				Files.write( jsonlineFilePath, lines, charset, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.error( programName + ":uploadOneCollection : Error concatenate to " + jsonlineFileWithPathName + " " + e.getMessage());
				throw new Exception("Error appending to file");
			}
	        
	        
	    }
		
		//Gzip jsonlinefiles. gzipJsonLineFile has path 
		String gzipJsonLineFile = TDMUtil.gzipFile( jsonlineFileWithPathName );  

		//upload gzipped jsonline files
		TDMUtil.UploadGzipJsonLineFile( gzipJsonLineFile );


		createdJsonLineFiles.add(jsonlineFileWithPathName);
		
	}

	private String processOneDocSouthXmlFile(String collectionid, String xmlfilename) throws Exception {
		
		logger.info( programName + ":processOneDocSouthXmlFile :process " + collectionid + " " + xmlfilename);
		
		String titleid = findTitleId( xmlfilename );
		
		V2DocSouthDocument docsouthDoc = new V2DocSouthDocument(collectionid, titleid);
		
		//use xml file to retreive metadata
		try {
			docsouthDoc.readMetadata(xmlfilename);
		} catch (Exception e) {
			failedTitles.add(titleid);
			failedJsonFiles.add(xmlfilename);
			logger.error( programName + ":processOneDocSouthXmlFile :readMetadata: " + collectionId + " " + titleid + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		//transform xml file into text file with pagebreaks
		/*try {
			docsouthDoc.transformXml2Txt();
		} catch (Exception e) {
			logger.error( programName + ":processOneDocSouthXmlFile: transformXml2Txt: " + collectionId + " " + titleid + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		*/
		
		//get full text from xml
		try {
			docsouthDoc.getFullTextFromTEIXml();
		}
		catch(Exception e) {
			failedTitles.add(titleid);
			failedJsonFiles.add(xmlfilename);
			logger.error( programName + ":processOneDocSouthXmlFile: getFullTextFromTEIXml: " + collectionId + " " + titleid + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		//use text file for TDM task
		try {
			docsouthDoc.performNLPTasks();
		} catch (IOException e) {
			failedTitles.add(titleid);
			failedJsonFiles.add(xmlfilename);
			logger.error( programName + ":processOneDocSouthXmlFile: performNLPTasks: " + collectionId + " " + titleid + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		//empty now
		docsouthDoc.logToDB();
		
		String jsonfileWithPath = null;
		try {
			jsonfileWithPath = docsouthDoc.outputJson();
		} catch (IOException e) {
			failedTitles.add(titleid);
			failedJsonFiles.add(xmlfilename);
			logger.error( programName + ":processOneDocSouthXmlFile: outputJson: " + collectionId + " " + titleid + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		
		processedTitles.add(titleid);
		createdJsonFiles.add(jsonfileWithPath);
		
		return jsonfileWithPath;
		
		
	}

	/**
	 * 
	 * @param xmlfilename	church-negrochurch-dubois.xml
	 * @return negrochurch
	 */
	private String findTitleId(String xmlfilename) {
		String titleid = null;
		
		Pattern p = Pattern.compile("(.*)-(.*)-(.*).xml");
		Matcher m = p.matcher(xmlfilename);
		
		if ( m.find()) {
			titleid = m.group(2);
		}
		
		
		return titleid;
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
        logger.info("Title processed =" + processedTitles.size());
        for( String title: processedTitles ) {
        	logger.info( "\t" + title);
        }
        logger.info("Title failed = " + failedTitles.size() ) ;
        for( String title: failedTitles ) {
        	logger.info( "\t" + title);
        }
        logger.info("Issue json file created =" + createdJsonFiles.size());
        for( String title: createdJsonFiles ) {
        	logger.info( "\t" + title);
        }
        logger.info("Issue json file not created = " + failedJsonFiles.size() ) ;
        for( String title: failedJsonFiles ) {
        	logger.info( "\t" + title);
        }
        
        if ( isUploadFlag()  ) {
        	logger.info("Successfully uploaded json line files (" + createdJsonLineFiles.size() + "): " );
        	for( String jsonlinefile: createdJsonLineFiles ) {
        		logger.info( "\t" + jsonlinefile);
        	}
        	
/*        	logger.info("Failed upload json line files (" + jsonLineFileFailedUpload.size() + "): " );
        	for( String jsonlinefile: jsonLineFileFailedUpload ) {
        		logger.info( "\t" + jsonlinefile);
        	}*/
 
        }
        
        logger.info("Total run time " + startTimeStr + "[start] to " + endTimeStr + "[end], in total = " + timeUsedStr );
        logger.info("================================================================");
		
	}
	

	private static void printUsage(Options options) {
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "V2DocSouthProcessor", options);  
		writer.flush();  
		
	}


	private static Options constructOptions() {
		final Options options = new Options();

		options.addOption(Option.builder("collection").required(false).hasArg().desc("Docsouth collection id, in lower case").build() );
		options.addOption(Option.builder("xml").required(false).hasArg().desc("Xml file name").build() );
		options.addOption(Option.builder("subdir").required(false).hasArg().desc("The subdirectory name under input directory where files have been stored.").build());
		options.addOption(Option.builder("upload").required(false).desc("upload to S3").build());
		options.addOption(Option.builder("combineby").required(false).hasArg().desc("How many json files are combined together").build());
		
		return options;
	}


	public static String getCollectionId() {
		return collectionId;
	}

	public static void setCollectionId(String collectionId) {
		V2DocSouthProcessor.collectionId = collectionId;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
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
	
	
	public String getDocsouth_collection_id() {
		return docsouth_collection_id;
	}

	public void setDocsouth_collection_id(String docsouth_collection_id) {
		this.docsouth_collection_id = docsouth_collection_id;
	}

	public boolean isUploadFlag() {
		return uploadFlag;
	}

	public void setUploadFlag(boolean uploadFlag) {
		this.uploadFlag = uploadFlag;
	}

	public List<String> getProcessedTitles() {
		return processedTitles;
	}

	public void setProcessedTitles(List<String> processedTitles) {
		this.processedTitles = processedTitles;
	}

	public List<String> getCreatedJsonFiles() {
		return createdJsonFiles;
	}

	public void setCreatedJsonFiles(List<String> createdJsonFiles) {
		this.createdJsonFiles = createdJsonFiles;
	}

	public List<String> getCreatedJsonLineFiles() {
		return createdJsonLineFiles;
	}

	public void setCreatedJsonLineFiles(List<String> createdJsonLineFiles) {
		this.createdJsonLineFiles = createdJsonLineFiles;
	}

	public List<String> getFailedTitles() {
		return failedTitles;
	}

	public void setFailedTitles(List<String> failedTitles) {
		this.failedTitles = failedTitles;
	}

	public List<String> getFailedJsonFiles() {
		return failedJsonFiles;
	}

	public void setFailedJsonFiles(List<String> failedJsonFiles) {
		this.failedJsonFiles = failedJsonFiles;
	}

	public List<String> getUploadedJsonFiles() {
		return uploadedJsonFiles;
	}

	public void setUploadedJsonFiles(List<String> uploadedJsonFiles) {
		this.uploadedJsonFiles = uploadedJsonFiles;
	}


}

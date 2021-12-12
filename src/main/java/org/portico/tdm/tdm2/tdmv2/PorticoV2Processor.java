package org.portico.tdm.tdm2.tdmv2;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
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
import org.portico.tdm.tdm2.datawarehouse.DWArticle;
import org.portico.tdm.tdm2.datawarehouse.DWIssue;
import org.portico.tdm.tdm2.datawarehouse.DWJournal;
import org.portico.tdm.tdm2.datawarehouse.DWVolume;
import org.portico.tdm.tdm2.tools.TDMUtil;



/**
 * This class is used to generate V2 tdm json files, which combine metadata and unigramCount in one file on article level.
 * Articles' json file will be saved under output/subdir/contentsetname/ directory.
 * AUs will be recorded in tdm_AU table to track the tdm process.
 * V2AWSLoader (another program) is used to compress them and upload to S3 location.
 * Usage:   
 * (linux copy-paste more_tdmenv first, or run . settdmenv.sh)
 *  java -Djava.util.logging.config.file=config/commons-logging.properties org.portico.tdm.tdmv2.PorticoV2Processor -subdir sage -cs ISSN_00144851
 * 	java org.portico.tdm.tdmv2.PorticoV2Processor  -cs ISSN_02664674 -v 34 -n 1 -subdir camb
 * 	java org.portico.tdm.tdmv2.PorticoV2Processor  -cs ISSN_02664674 -subdir camb -auid ark:/27927/phw235mnh66
 * 	java org.portico.tdm.tdmv2.PorticoV2Processor -subdir sage -cs ISSN_26318318
 * 	java org.portico.tdm.tdmv2.PorticoV2Processor -subdir sage -cs  ISSN_21568693 -v 1 -n 1
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -subdir sage -cs ISSN_21568693 -auid ark:/27927/phz2hk17jr8
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -subdir sage -cs ISSN_00144851  ( 30 AUs, good for testing) > /dev/null 2>&1
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -subdir led -publisher LED
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -subdir begell -cs ISSN_21658420 -auid ark:/27927/phzd003jfmg (multiple html)
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -subdir wiley -csfile wiley_1_aa
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -book  -publisher EUP  -subdir eup  -auid ark:/27927/pbd16v8g8b7
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -book  -publisher EUP  -subdir eup 
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -book  -publisher EUP  -subdir eup  -aufile au_list
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -book  -publisher MUSE  -subdir muse -auid ark:/27927/phz2cf8m90s  (no body in xml file) 
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -book  -publisher SAGE  -subdir sage -cs "SAGE RESEARCH METHODS"
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -book  -publisher SAGE  -subdir sage -cs "SAGE KNOWLEDGE"
 *  java org.portico.tdm.tdmv2.PorticoV2Processor -subdir sage -publisher SAGE -scandir  (process what is under sage/ directory)  
 *  nohup java org.portico.tdm.tdmv2.PorticoV2Processor -newcontent  -subdir newcontent_202104 > /dev/null 2>&1 &
 *  nohup java org.portico.tdm.tdmv2.PorticoV2Processor -subdir academicus -publisher ACADEMICUS > /dev/null 2>&1 &
 *  
 * @author dxie
 * @version 1.1 7/21/2020 Added -scandir option to allow only process newly exported AUs under each content set directory
 * 			1.2 2/17/2021 Added V2Article.retrieveArticleTypeFromJatsXml() to retrieve @article-type from Jats xml file and save to tdm_au.article_type table.
 * 							This was done by SamplePorticoAU.java, now included in the creating of tdm_au work process.
 * 			1.3 3/9/2021	Add new content workflow. All publishers' input read from same location, tdm/input/newcontent_yyyyMM, and created .json files output to same location, tdm/output/newcontent_yyyyMM.
 * 			1.4 10/25/2021  Added Books' OA check.
 *
 */
public class PorticoV2Processor {
	
	static Properties props = new Properties();
	static Logger logger = LogManager.getLogger(PorticoV2Processor.class.getName());
	static String programName = "PorticoV2Processor";
	
	static {
		java.util.logging.Logger.getLogger("org.apache.pdfbox").setLevel(java.util.logging.Level.OFF);
	}
	
	String outputDir = "output";
	String inputDir = "input";
	String configDir = "config";
	String cacheDir = "data" + File.separator + "portico_cache";
	String subDir = "";
	String server = "PROD";
	boolean bookType = false;
	boolean journalType = true;
	boolean scandir = false;
	
	long starttime, endtime;
	String startTimeStr, endTimeStr;
	String argStr;
	List<String> processedContentSetList;
	List<String> processedContentSetIssueList;
	List<String> processedArticleOrBookList;
	int processedChapterCount;
	List<String> fulltextFromPdf;
	List<String> fulltextFromXml;
	List<String> fulltextFromHTML;
	List<String> fulltextFromXmlHeader;
	List<String> nofulltext;
	List<String> missingAU;
	int processedAUCount = 0;
	int skippedAUCount = 0;
	
	int counter = 0;
	
	public PorticoV2Processor() {
		//get startTime
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		startTimeStr = dateFormat.format(cal.getTime());
		starttime = System.currentTimeMillis();
		
		processedContentSetList = new ArrayList<>();
		processedContentSetIssueList = new ArrayList<>();
		processedArticleOrBookList = new ArrayList<>();
		processedChapterCount = 0;
		
		fulltextFromPdf = new ArrayList<>();
		fulltextFromXml = new ArrayList<>();
		fulltextFromHTML = new ArrayList<>();
		fulltextFromXmlHeader = new ArrayList<>();
		nofulltext = new ArrayList<>();
		missingAU = new ArrayList<>();
		
		counter = 0;
		
		try {
			System.out.println("all versions of log4j Logger: " + getClass().getClassLoader().getResources("org/apache/log4j/Logger.class") );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		
		
		
		//System.setProperty("org.apache.fontbox.util.autodetect.FontFileFinder.DEFAULT_CUTOFF_LEVEL", "ERROR");
		//System.setProperty("org.apache.pdfbox.pdmodel.font.PDSimpleFont.DEFAULT_CUTOFF_LEVEL", "ERROR");
		System.setProperty("org.apache.fontbox.util.autodetect.FontFileFinder.LEVEL", "OFF");
		System.setProperty("org.apache.pdfbox.pdmodel.font.PDSimpleFont.LEVEL", "OFF");
		java.util.logging.Logger
	    .getLogger("org.apache.pdfbox").setLevel(java.util.logging.Level.OFF);
		
		String[] loggers = { "org.apache.pdfbox.util.PDFStreamEngine",
        					"org.apache.pdfbox.pdmodel.font.PDSimpleFont" };

		/*for (String logger : loggers) {
				org.apache.log4j.Logger logpdfengine = org.apache.log4j.Logger.getLogger(logger);
				logpdfengine.setLevel(org.apache.log4j.Level.OFF);
		}
		*/
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();

		PorticoV2Processor processor = new PorticoV2Processor(  );

		// parse the command line arguments
		try {
			CommandLine line = parser.parse( options, args );
			String publisher;
			String cs;
			
			logger.info( programName + " with args: " + String.join(" ", args));
			processor.setArgStr( String.join(" ", args)) ;

			if ( line.hasOption("book")) {
				processor.setBookType(true);
				processor.setJournalType(false);
			}
			else {
				processor.setJournalType(true);
				processor.setBookType(false);
			}

			if ( line.hasOption("subdir")) {		//required for all input
				processor.setSubDir(line.getOptionValue("subdir"));
			}
			else {
				PorticoV2Processor.printUsage( options );
				System.exit(-1);
			}
			
			
			//books
			if ( line.hasOption("publisher") && line.hasOption("book") ) {
				
				publisher = line.getOptionValue("publisher");
				
				boolean hasOABookPublisher = false;
				
				if ( TDMUtil.isHasOABookPublisher(publisher)) {
					hasOABookPublisher = true;
				}
				
				if ( line.hasOption("auid")) {
					String auid = line.getOptionValue("auid");
					processor.processV2ABookAU( publisher, auid, hasOABookPublisher, 1 );
				}
				else if (line.hasOption("aufile") ) {
					String aufilename = line.getOptionValue("aufile");

					processor.processV2BookAUsInAFile( publisher, aufilename, hasOABookPublisher );
				}
				else if (line.hasOption("cs")) {
					String csname = line.getOptionValue("cs");
					
					processor.processV2BookOfAContentSet( publisher, csname );
				}
				else {
					processor.processV2BooksOfPublisher( publisher );
				}
				
				
			}
			
			//following are for journals
			else if ( line.hasOption("publisher") && ! line.hasOption("book")) {
				publisher = line.getOptionValue("publisher");
				
				if ( line.hasOption("scandir")) {
					processor.setScandir(true);
					processor.processV2ContentSetsForAPublisherInDir( publisher );
				}
				else {
					processor.setScandir(false);
					processor.processV2ContentSetsForAPublisher( publisher );
				}

			}
			else if ( line.hasOption("cs")) {
				cs = line.getOptionValue("cs");

				
				if ( line.hasOption("v") & line.hasOption("n")) {
					String volumeNo = line.getOptionValue("v");
					String issueNo = line.getOptionValue("n");

						processor.processV2OneContentSetIssue( cs,  volumeNo, issueNo );

				}
				else if ( line.hasOption("auid")) {
					String auid = line.getOptionValue("auid");
					
					processor.processV2OneAuid( cs, auid, false );
				}
				else if ( line.hasOption("scandir")) {
					processor.processV2OneContentSetDir(cs );
				}
				else {

					processor.processV2OneContentSet( cs );
				}

				
			}
			else if ( line.hasOption("csfile")) {
				String filename = line.getOptionValue("csfile");

				processor.processV2ContentSetsInAFile( filename );

				
			}
			else if ( line.hasOption("issuefile")) {
				String filename = line.getOptionValue("issuefile");

				processor.processV2ContentSetIssuesInAFile( filename );
			}
			else if ( line.hasOption("newcontent")) {
				processor.processV2NewContent();
			}
			else {
				PorticoV2Processor.printUsage( options );
			}
			
			processor.printStats();


		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}



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
        logger.info("Processing summary: " );
        logger.info("Successfully processed content sets (" + processedContentSetList.size() + "): " );
        for( String cs: processedContentSetList ) {
        	logger.info( "\t" + cs);
        }
        logger.info("Successfully processed content set issue (" + processedContentSetIssueList.size() + "): " );
        for( String issueId: processedContentSetIssueList ) {
        	logger.info( "\t" + issueId);
        }
        logger.info("Successfully processed articles or books (" + processedArticleOrBookList.size() + "): " );
        logger.info("Article/book full text extracted from pdf file (" + fulltextFromPdf.size() + "): " );
        logger.info("Article/book full text extracted from xml file (" + fulltextFromXml.size() + "): " );
        logger.info("Article/book full text extracted from html file (" + fulltextFromHTML.size() + "): " );
        logger.info("Article/book full text extracted from xml header file (" + fulltextFromXmlHeader.size() + "): " );
        logger.info("Totoal chapters been processed =" + processedChapterCount );
        logger.info("Article/book without full text (" + nofulltext.size() + "): " );
        for(String auid: nofulltext) {
        	logger.info("\t" + auid);
        }
        logger.info("AUs do not exist (" + missingAU.size() + "): " );
        for(String auid: missingAU) {
        	logger.info("\t" + auid);
        }
        logger.info("Duplicate articles = " + skippedAUCount);
        logger.info("Total process time " + startTimeStr + "[start] to " + endTimeStr + "[end], in total = " + timeUsedStr );
        logger.info("================================================================");
        
        
	}
	
	
	/**
	 * This method processes newly added journal content from tdm/input/newcontent_yyyyMM directory and outputs json files to tdm/output/newcontent_yyyyMM directory.
	 * Caching xml files is also included.
	 */
	private void processV2NewContent() {
		
		Date snapshotDate = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMM");
		String reportDateStr = dateFormat.format(snapshotDate);
		String subdir = "newcontent_" + reportDateStr;
		setSubDir(subdir);
		
		String newContentDir = inputDir + File.separator + subdir;
		
		setSubDir("newcontent_" + reportDateStr);

		
		//read all cs directory names
		List<String> csnames = new ArrayList<>();

		File[] directories = new File(newContentDir).listFiles(File::isDirectory);
		
		for(File csdir: directories) {
			String dirname = csdir.toString();
			String csname = dirname.substring(dirname.lastIndexOf(File.separator)+1);

			csnames.add(csname);
		}
		
		Collections.sort(csnames);
		
		counter =0;
		
		for(String csdir: csnames) {
			try {
				processV2OneContentSetDir(csdir);
			}
			catch(Exception e) {
				
			}
		}
		
	}


	/**
	 * 
	 * @param cs
	 * @param au_id
	 * @param checkDupFlag If true, will check duplication_flag of this article. If 'Y', then do not process. If false, then don't check.
	 * @throws Exception
	 */
	private void processV2OneAuid(String cs, String au_id, boolean checkDupFlag) throws Exception {
		
		//logger.info(programName + ":processV2OneAuid for " + cs + " au_id: " + au_id );
		
		DWJournal journal = new DWJournal(cs);
		try {
			journal.populateDWJournal();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error( programName + ":processV2OneAuid: populateDWJournal " + cs + " " + e.getMessage());
			throw e;
		}
		
		String publisher = journal.getPublisher();
		String publisherID = journal.getPublisherID();
		String journal_title = journal.getJournal_title();
		
		DWArticle article = new DWArticle( au_id );
		try {
			article.populateDWArticle();
		} catch (Exception e1) {
			logger.info( programName + ":processV2OneAuid :" + au_id + "(" + article.getArticle_title() + ") :");
			e1.printStackTrace();
			throw e1;
		}
		
		if ( checkDupFlag ) {
			if ( article.getDuplication_flag()!= null && article.getDuplication_flag().equals("Y")) {
				logger.info( programName + ":processV2OneAuid :" + au_id + "(" + article.getArticle_title() + ") :duplicate article. Do not process");
				throw new Exception("duplicate article");
			}
		}
		article.setJournal_ark_id(journal.getArkId());
		
		int articleIndex = article.getArticle_seq();
		String volNo = article.getVol_no();
		String issueNo = article.getIssue_no();
		String issueId = cs + " v." + volNo + " n." + issueNo;
		
		DWIssue issue = new DWIssue(cs, volNo, issueNo );
		try {
			issue.populateDWIssue();
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error( programName + ":processV2OneAuid: populateDWIssue " + " " + issueId + " " + e.getMessage());
			throw e;
		}
		
		String issue_arkid = issue.getArkId();
		article.setIssue_ark_id(issue_arkid);
		
		V2Article v2article = new V2Article( au_id );

		if ( journal.getHasOA().equalsIgnoreCase("yes")) {
			v2article.setFullTextAvailable( true );	//if it is OA title
			v2article.addOAToOutputFormat();
		}
		else {
			v2article.setFullTextAvailable( false );
		}

		v2article.createV2ObjectFromDWArticle( article );
		v2article.setPublisher(publisher);
		v2article.setPublisherId(publisherID);
		v2article.setIsPartOf(journal_title);
		v2article.setSequence(articleIndex);
		v2article.setOutputDir(outputDir);
		v2article.setInputDir(inputDir);
		v2article.setSubDir(subDir);
		
		//retrieve article type from jats xml file
		try {
			v2article.retrieveArticleTypeFromJatsXml();
		} catch (Exception e) {
			logger.error( programName + ":processV2OneAuid: V2Article error getting article type from xml file " + cs + " " + au_id + e);
			e.printStackTrace();
		}


		try {
			v2article.performNLPTasks( );

		} catch (Exception e) {
			logger.error( programName + ":processV2OneAuid:performNLPTasks: " + cs + " " + au_id + e);
			if ( e.getMessage().indexOf("") != -1 ) {
				missingAU.add(issueId + ":" + au_id);
			}
			e.printStackTrace();
			throw e;
		}
		
		//find dup AUs and delete(mark) them in tdm_au
		try {
			deleteDupTdmAu(au_id, cs);
		} catch (Exception e) {
			logger.error( programName + ":processV2OneAuid: deleteDupTdmAu: " + cs + " " + au_id + e);
			e.printStackTrace();
			//throw e;
		}

		//log some AU info in TDM_AU table
		try {
			v2article.logToDB();
		} catch (Exception e) {
			logger.error( programName + ":processV2OneAuid: logToDB: " + cs + " " + au_id + e);
			e.printStackTrace();
			throw e;
		}

		try {
			v2article.outputJson();
		} catch (IOException e) {
			logger.error( programName + ":processV2OneAuid:outputJson: " + cs + " " + au_id + e);
			e.printStackTrace();
			throw e;
		}

		processedArticleOrBookList.add(au_id);
		
		String getFullTextFrom = v2article.getFulltextFrom();
		
		if ( getFullTextFrom != null ) {
			if ( getFullTextFrom.equalsIgnoreCase("pdf") ) {
				fulltextFromPdf.add(au_id);
			}
			else if (getFullTextFrom.equalsIgnoreCase("xml")  ) {
				fulltextFromXml.add(au_id);
			}
			else if ( getFullTextFrom.equalsIgnoreCase("html") ) {
				fulltextFromHTML.add(au_id);
			}
			else if ( getFullTextFrom.equalsIgnoreCase("xml header") ) {
				fulltextFromXmlHeader.add(au_id);
			}
			else if ( getFullTextFrom.equalsIgnoreCase("no fulltext") ) {
				nofulltext.add(au_id);
			}
		}
		
	}

	/**
	 * Sometimes dw_article.duplication_flag may toggle between articles.
	 * Need to make sure old tdm_au entry be deleted (article_type->'Not exist') if new entry will be inserted.
	 * AWS also need to be deleted in future.
	 * @param au_id
	 * @param csname 
	 * @throws Exception 
	 */
	private void deleteDupTdmAu(String au_id, String csname) throws Exception {

		String query1 = "select tdm_au.au_id from tdm_au, dw_article where dw_article.issn_no='" + csname + "' and dw_article.dup_of_article_auid='" + au_id  +"' and tdm_au.au_id=dw_article.au_id";
		String query2 = "update tdm_au set article_type='Not exist' where content_set_name='" + csname + "' and au_id=?";
		
		try(Connection conn = TDMUtil.getConnection("PROD");
				Statement stmt1 = conn.createStatement();
				PreparedStatement stmt2 = conn.prepareStatement(query2); ) {
			
			ResultSet rs = stmt1.executeQuery(query1);
			
			while( rs.next()) {
				String dup_au_id = rs.getString("au_id");
				
				stmt2.setString(1, dup_au_id);
				
				stmt2.executeQuery();
			}
			
			
		}
		catch(Exception e) {
			logger.error( programName + ":deleteDupTdmAu :Error delete/mark duplicate AU in tdm_au table for au_id " + au_id + ". " + e.getMessage());
			throw e;	
		}
		
	}

	private void processV2ContentSetsForAPublisher(String publisher) throws Exception {
		
		List<String> csnames = null;
		TDMUtil util = new TDMUtil();
		
		Connection conn = TDMUtil.getConnection("PROD");
		
		try {
			csnames = util.getJournalListForAPublisher(conn, publisher);
		}
		catch(Exception e) {
			logger.error( programName + ":processV2ContentSetsForAPublisher for " + publisher + ". " + e.getMessage());
			throw e;
		}
		finally {
			conn.close();
		}
		
		for( String cs: csnames ) { 
			
			processV2OneContentSet( cs );

		}
	}
	
	

	/**
	 * Scan subdir to get content set names to process, scan each content set directory to get AUs to process
	 * @param publisher
	 */
	private void processV2ContentSetsForAPublisherInDir(String publisher) {

		List<String> csdirs = scanPublisherDir(publisher);
		logger.info( programName + ": processV2ContentSetsForAPublisherInDir : Publisher " + publisher + " total directory = " + csdirs.size() );
		
		for(String csdir: csdirs ) {
			
			processV2OneContentSetDir( csdir );
		}
		
	}




	private List<String> scanPublisherDir(String publisher) {
		List<String> CSs = new ArrayList<>();
		
		String dir = inputDir + File.separator + getSubDir() ;
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
	 * 
	 * @param filename file is under config/ directory.
	 */
	private void processV2ContentSetIssuesInAFile(String filename) {
		
		List<String> lines = null;
		
		try {
			lines = TDMUtil.getLineContentFromFile(configDir + File.separator + filename);
		}
		catch(Exception e) {
			logger.error( programName + "processV2ContentSetIssuesInAFile:getJournalsFromFile " + filename + ". " + e.getMessage());
			return;
		}
		
		
		for(String line:lines) {
			if ( line.indexOf("\t") == -1 ) {
				logger.error(programName + "processV2ContentSetIssuesInAFile: invalid format of line " + line + ". Use content set name\tvolume number\tissue number.");
				continue;
			}
			String[] data = line.split("\t");
			
			String issn_no = data[0];
			String vol_no = data[1];
			String issue_no = data[2];
			
			if ( issn_no == null || vol_no == null || issue_no == null ) {
				logger.error(programName + "processV2ContentSetIssuesInAFile: invalid format of line " + line + ". Use content set name\tvolume number\tissue number.");
				continue;
			}
			
			try {
				processV2OneContentSetIssue(issn_no, vol_no, issue_no);
			} catch (Exception e) {
				logger.error(programName + "processV2ContentSetIssuesInAFile: Error processing "+ issn_no + " v." + vol_no + " n." + issue_no + ". " + e.getMessage());
				continue;
			}

			
		}

		
		
	}


	/**
	 * 
	 * @param filename file is under config/ directory
	 */
	private void processV2ContentSetsInAFile(String filename) {
		List<String> csnames = null;
		
		try {
			csnames = TDMUtil.getLineContentFromFile(configDir + File.separator +  filename);
		}
		catch(Exception e) {
			logger.error( programName + ":processV2ContentSetsInAFile: getJournalsFromFile " + filename + ". " + e.getMessage());
			return;
		}
		
		for( String cs: csnames ) { 
			
			try {
				processV2OneContentSet( cs );
			}
			catch(Exception e) {
				logger.error( programName + ":processV2ContentSetsInAFile: processV2OneContentSet " + cs + ". " + e.getMessage());
			}

		}
		
	}


	/**
	 * Create v2 json file for each issue in a content set.
	 * @param cs
	 * @throws Exception 
	 */
	private void processV2OneContentSet(String cs) throws Exception {
		
		logger.info(programName + ":processV2OneContentSet for " + cs );
		
		DWJournal journal = new DWJournal(cs);
		try {
			journal.populateDWJournal();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error( programName + ":processV2OneContentSet:populateDWJournal " + cs + " " + e.getMessage());
			throw e;
		}
		
		try {
			journal.findAllArchivedVolumes();
		} catch (Exception e1) {
			e1.printStackTrace();
			logger.error( programName + ":processV2OneContentSet:findAllArchivedVolumes " + cs + " " + e1.getMessage());
			throw e1;
		}
		
		String publisher = journal.getPublisher();
		String publisherID = journal.getPublisherID();
		String journal_title = journal.getJournal_title();
		String subDir = getSubDir();
		String journal_arkid = journal.getArkId();
		
		List<DWVolume> volumes = journal.getVolumes();
		
		for( DWVolume volume: volumes ) {
			String vol_no = volume.getVolumeNumber();
			try {
				volume.findAllArchivedIssuesInVolume( );
			} catch (Exception e) {
				e.printStackTrace();
				logger.error( programName + ":processV2OneContentSet:findAllArchivedIssuesInVolume " + cs + " v." + vol_no + " " + e.getMessage());
				throw e;
			}
			
			List<DWIssue> issues = volume.getIssues();
			
			for( DWIssue issue: issues ) {
				String issue_no = issue.getIssue_no();
				String issueId = cs + " v." + vol_no + " n." + issue_no;
				
				logger.info(programName + ":processV2OneContentSet: process issue " + issueId );
				
				try {
					issue.populateDWIssue();
				}
				catch(Exception e) {
					e.printStackTrace();
					logger.error( programName + ":processV2OneContentSet: populateDWIssue " + " " + issueId + " " + e.getMessage());
					continue;
				}
				
				String issue_arkid = issue.getArkId();
			
				try {
					issue.findAndPopulateMetadataForAllArticlesInIssue( );
				} catch (Exception e) {
					e.printStackTrace();
					logger.error( programName + ":processV2OneContentSet:findAndPopulateMetadataForAllArticlesInIssue " + " " + issueId + " " + e.getMessage());
					continue;
				}
				
				//fix muse pub_year issue
				try {
					issue = fixIssuePubDate(publisherID, issue);

				} catch (Exception e) {
					e.printStackTrace();
					logger.error( programName + ":processV2OneContentSet: fixIssuePubDate " + " " + issueId + " " + e.getMessage());
					continue;
				}

				
				List<DWArticle> articles = issue.getArticles();
				
				//For each ordered/deduped DWArticle, create a V2Article object and output its json file
				int articleIndex = 0;
				for(DWArticle article: articles) {
					
					String au_id = article.getAu_id();
					article.setJournal_ark_id(journal_arkid);
					article.setIssue_ark_id(issue_arkid);
					
					articleIndex ++;
					
					V2Article v2article = new V2Article( au_id );
					
					if ( journal.getHasOA().equalsIgnoreCase("yes")) {
						v2article.setFullTextAvailable( true );	//if it is OA title
						v2article.addOAToOutputFormat();
					}
					else {
						v2article.setFullTextAvailable( false );
					}
					
					v2article.createV2ObjectFromDWArticle( article );
					v2article.setPublisher(publisher);
					v2article.setPublisherId(publisherID);
					v2article.setIsPartOf(journal_title);
					v2article.setSequence(articleIndex);
					v2article.setOutputDir(outputDir);
					v2article.setInputDir(inputDir);
					v2article.setSubDir(subDir);

					//retrieve article type from jats xml file
					try {
						v2article.retrieveArticleTypeFromJatsXml();
					} catch (Exception e) {
						logger.error( programName + ":processV2OneContentSet: V2Article error getting article type from xml file " + cs + " " + au_id + e);
						e.printStackTrace();
					}

					try {
						v2article.performNLPTasks( );
					} catch (Exception e) {
						logger.error( programName + ":processV2OneContentSet: performNLPTasks: " + cs + " " + au_id + e);
						if ( e.getMessage().indexOf("") != -1 ) {
							missingAU.add(issueId + ":" + au_id);
						}
						continue;
					}
					
					//log some AU info in TDM_AU table
					try {
						v2article.logToDB();
					} catch (Exception e) {
						logger.error( programName + ":processV2OneContentSet: logToDB: " + cs + " " + au_id + e);
						e.printStackTrace();

					}
					
					try {
						v2article.outputJson();
					} catch (Exception e) {
						logger.error( programName + ":processV2OneContentSet: outputJson " + " " + issueId + ":au id: " + au_id + " " + e.getMessage());
						e.printStackTrace();
						
					}

					processedArticleOrBookList.add(au_id);
					
					String getFullTextFrom = v2article.getFulltextFrom();
					
					if ( getFullTextFrom != null ) {
						if ( getFullTextFrom.equalsIgnoreCase("pdf") ) {
							fulltextFromPdf.add(au_id);
						}
						else if (getFullTextFrom.equalsIgnoreCase("xml")  ) {
							fulltextFromXml.add(au_id);
						}
						else if ( getFullTextFrom.equalsIgnoreCase("html") ) {
							fulltextFromHTML.add(au_id);
						}
						else if ( getFullTextFrom.equalsIgnoreCase("xml header") ) {
							fulltextFromXmlHeader.add(au_id);
						}
						else if ( getFullTextFrom.equalsIgnoreCase("no fulltext") ) {
							nofulltext.add(au_id);
						}
					}
					

				}
				
				processedContentSetIssueList.add(issueId);
								
			}
			
			
		}
		
		processedContentSetList.add(cs);
		
	}
	
	

	/**
	 * This method scans a cs directory and process all AUs under it. It also caches the AUs.
	 * @param cs
	 */
	public int processV2OneContentSetDir(String cs) {
		
		counter ++;

		List<String> AUs = scanCSDirectoryToGetAus( cs );
		
		if ( AUs == null || AUs.isEmpty()) {
			logger.info( programName + ":processV2OneContentSetDir : " + counter + ", " + cs + ": No AU to process");
			return 0;
		}

		int dupArticleCount = 0;
		int newArticleCount = 0;
		for(String auid: AUs ) {
			try {
				processV2OneAuid( cs, auid, true );		//if it is a duplicate article, then skip process
				processedAUCount++;
				newArticleCount++;
			} 
			catch (Exception e) {
				if ( e.getMessage().equals("duplicate article")) {
					skippedAUCount ++;
					dupArticleCount ++;
				}
				else {
					skippedAUCount ++;
				}
			}
			
			//cache this AU (even it is duplicate and didn't process)
			try {
				cacheOneAUByMonth( cs, auid );
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		logger.info( programName + ":processV2OneContentSetDir : " + counter + ", " + cs + ": new article = " + newArticleCount + ", duplicate  = " + dupArticleCount);
		
		return newArticleCount;
		
	}


	/**
	 * For new content 2021.3 and after, cache them by month.
	 * Logic is same as cacheOneAU, but cache directory has been changed and also need to write to tdm_au.cache_dir field.
	 * @param cs
	 * @param auid
	 * @throws Exception 
	 */
	private void cacheOneAUByMonth(String cs, String auid) throws Exception {
		
		
		String bitsXmlFileName = null;
		
		//we want au without ark id
		String au = auid.replace("ark:/27927/", "");	//au: without ark id
		
		if ( ! auid.startsWith("ark")) {				//auid: with ark id
			auid = "ark:/27927/" + auid;
		}
		
		try {
			bitsXmlFileName = TDMUtil.getBitsXmlFileNameForAU(auid);
		} catch (Exception e) {
			logger.error( programName + ":cacheOneAUByMonth: Error getting BITS xml file for " + auid);
			e.printStackTrace();
			throw e;
		}
		
		
		//constructs full xml file name
		String fullBitsXmlFileName = getInputDir() + File.separator + getSubDir() +  File.separator + cs +  File.separator + au + File.separator + "data" + File.separator + bitsXmlFileName;

		//use new location
		//String cacheXmlFileDir = getCacheDir() + File.separator  + au + File.separator + "data";
		Calendar c = Calendar.getInstance(); 
		int this_month = c.get(Calendar.MONTH) + 1; // beware of month indexing from zero
		int this_year  = c.get(Calendar.YEAR);
		
		String cacheXmlFileDir = getCacheDir() + "_" + (this_year + ( this_month<10? "0" + this_month: "" + this_month )) + File.separator  + au + File.separator + "data";
		
		try {
			Files.createDirectories(Paths.get(cacheXmlFileDir));
		} catch (IOException e1) {
			logger.error( programName + ": cacheOneAUByMonth: Error creating new directory " + cacheXmlFileDir + " " + e1.getMessage());
			throw e1;
		}
		
		String cacheXmlFileName = cacheXmlFileDir + File.separator + bitsXmlFileName;
		
        //copy xml file to cache directory 
		try {
			Files.copy(Paths.get(fullBitsXmlFileName), Paths.get(cacheXmlFileName), StandardCopyOption.REPLACE_EXISTING);
			
		} catch (IOException e) {
			logger.error( programName + ": cacheOneAUByMonth: Error moving file from " + fullBitsXmlFileName + " to " + cacheXmlFileName + " " + e.getMessage());
			throw e;
		}
		
		
        
		//delete original AU subdirectory

		TDMUtil.deleteDir(new File(getInputDir() +  File.separator + getSubDir() +  File.separator + cs +  File.separator  + au));
		
		logger.info( programName +":cacheOneAUByMonth: moved " + fullBitsXmlFileName + " to " + cacheXmlFileName + " and deleted " + (getInputDir() +  File.separator + getSubDir() +  File.separator + cs +  File.separator  + au) + " directory." );

		
	}

	/**
	 * This is a slight different version from CachePorticoAU.cacheOneAU.
	 * This is used before 2021.09
	 * @param cs
	 * @param auid
	 * @throws Exception 
	 */
	private void cacheOneAU(String cs, String auid) throws Exception {
		
		//query DB to find out active XML file name
		String bitsXmlFileName = null;
		
		//we want au without ark id
		String au = auid.replace("ark:/27927/", "");	//au: without ark id
		
		if ( ! auid.startsWith("ark")) {				//auid: with ark id
			auid = "ark:/27927/" + auid;
		}
		
		try {
			bitsXmlFileName = TDMUtil.getBitsXmlFileNameForAU(auid);
		} catch (Exception e) {
			logger.error( programName + ":cacheOneAU: Error getting BITS xml file for " + auid);
			e.printStackTrace();
			throw e;
		}
		
		
		//constructs full xml file name
		String fullBitsXmlFileName = getInputDir() + File.separator + getSubDir() +  File.separator + cs +  File.separator + au + File.separator + "data" + File.separator + bitsXmlFileName;
		String cacheXmlFileDir = getCacheDir() + File.separator  + au + File.separator + "data";
		
		try {
			Files.createDirectories(Paths.get(cacheXmlFileDir));
		} catch (IOException e1) {
			logger.error( programName + ": cacheOneAU: Error creating new directory " + cacheXmlFileDir + " " + e1.getMessage());
			throw e1;
		}
		
		String cacheXmlFileName = cacheXmlFileDir + File.separator + bitsXmlFileName;
		
        //copy xml file to cache directory 
		try {
			Files.copy(Paths.get(fullBitsXmlFileName), Paths.get(cacheXmlFileName), StandardCopyOption.REPLACE_EXISTING);
			
		} catch (IOException e) {
			logger.error( programName + ": cacheOneAU: Error moving file from " + fullBitsXmlFileName + " to " + cacheXmlFileName + " " + e.getMessage());
			throw e;
		}
        
		//delete original AU subdirectory
		//if ( isDeleteFlag() ) {
			TDMUtil.deleteDir(new File(getInputDir() +  File.separator + getSubDir() +  File.separator + cs +  File.separator  + au));
		
			logger.info( programName +":cacheOneAU: moved " + fullBitsXmlFileName + " to " + cacheXmlFileName + " and deleted " + (getInputDir() +  File.separator + getSubDir() +  File.separator + cs +  File.separator  + au) + " directory." );
		//}
		//else {
		//	logger.info(index + " of " + total_count + ", moved " + fullBitsXmlFileName + " to " + cacheXmlFileName + ".");
		//}
		
		
	}

	private List<String> scanCSDirectoryToGetAus(String csname) {
		List<String> AUs = new ArrayList<>();
		
		String dir = inputDir + File.separator + getSubDir() + File.separator + csname;
		
		logger.info(dir);
		File[] directories = new File(dir).listFiles(File::isDirectory);
		for(File audir: directories) {
			String dirname = audir.toString();
			String au = "ark:/27927/" + dirname.substring(dirname.lastIndexOf(File.separator)+1);
			AUs.add(au);
			//System.out.println(au);
		}
		
		
		
		return AUs;
	}

	private void processV2OneContentSetIssue(String cs, String volumeNo, String issueNo) throws Exception {
		
		String issueId = cs + " v." + volumeNo + " n." + issueNo;
		logger.info(programName + ":processV2OneContentSet: process issue " + issueId );
		
		DWJournal journal = new DWJournal(cs);
		try {
			journal.populateDWJournal();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error( programName + ":processV2OneContentSetIssue:populateDWJournal " + cs + " " + e.getMessage());
			throw e;
		}
		
	
		String publisher = journal.getPublisher();
		String publisherID = journal.getPublisherID();
		String journal_title = journal.getJournal_title();
		String subDir = getSubDir();
		String journal_arkid = journal.getArkId();
		
		DWIssue issue = new DWIssue( cs, volumeNo, issueNo );
		
		try {
			issue.populateDWIssue();
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error( programName + ":processV2OneContentSetIssue: populateDWIssue " + " " + issueId + " " + e.getMessage());
			throw e;
		}
		
		String issue_arkid = issue.getArkId();

		try {
			issue.findAndPopulateMetadataForAllArticlesInIssue( );
		} catch (Exception e) {
			e.printStackTrace();
			logger.error( programName + ":processV2OneContentSetIssue: findAndPopulateMetadataForAllArticlesInIssue " + " " + issueId + " " + e.getMessage());
			throw e;
		}
		
		//fix muse pub_year issue
		try {
			issue = fixIssuePubDate(publisherID, issue);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error( programName + ":processV2OneContentSetIssue: fixIssuePubDate " + " " + issueId + " " + e.getMessage());
		}


		List<DWArticle> articles = issue.getArticles();

		//For each ordered/deduped DWArticle, create a V2Article object and output its json file
		int articleIndex = 0;
		for(DWArticle article: articles) {

			String au_id = article.getAu_id();
			article.setJournal_ark_id(journal_arkid);
			article.setIssue_ark_id(issue_arkid);
			
			articleIndex ++;

			V2Article v2article = new V2Article( au_id );

			if ( journal.getHasOA().equalsIgnoreCase("yes")) {
				v2article.setFullTextAvailable( true );	//if it is OA title
				v2article.addOAToOutputFormat();
			}
			else {
				v2article.setFullTextAvailable( false );
			}

			v2article.createV2ObjectFromDWArticle( article );
			v2article.setPublisher(publisher);
			v2article.setPublisherId(publisherID);
			v2article.setIsPartOf(journal_title);
			v2article.setSequence(articleIndex);
			v2article.setOutputDir(outputDir);
			v2article.setInputDir(inputDir);
			v2article.setSubDir(subDir);

			//retrieve article type from jats xml file
			try {
				v2article.retrieveArticleTypeFromJatsXml();
			} catch (Exception e) {
				logger.error( programName + ":processV2OneContentSetIssue: V2Article error getting article type from xml file " + cs + " " + au_id + e);
				e.printStackTrace();
			}
			
			try {
				v2article.performNLPTasks( );
			} catch (Exception e) {
				logger.error( programName + ":processV2OneContentSetIssue: performNLPTasks " + " " + issueId + " " + e.getMessage());
				if ( e.getMessage().indexOf("") != -1 ) {
					missingAU.add(issueId + ":" + au_id);
				}
				continue;
			}
			
			//log some AU info in TDM_AU table
			try {
				v2article.logToDB();
			} catch (Exception e) {
				logger.error( programName + ":processV2OneContentSetIssue: logToDB: " + cs + " " + au_id + e);
				e.printStackTrace();

			}

			try {
				v2article.outputJson();
			} catch (Exception e) {
				logger.error( programName + ":processV2OneContentSetIssue: outputJson " + " " + issueId + " " + e.getMessage());
				e.printStackTrace();
			}

			processedArticleOrBookList.add(au_id);
			
			String getFullTextFrom = v2article.getFulltextFrom();
			
			if ( getFullTextFrom != null ) {
				if ( getFullTextFrom.equalsIgnoreCase("pdf") ) {
					fulltextFromPdf.add(au_id);
				}
				else if (getFullTextFrom.equalsIgnoreCase("xml")  ) {
					fulltextFromXml.add(au_id);
				}
				else if ( getFullTextFrom.equalsIgnoreCase("html") ) {
					fulltextFromHTML.add(au_id);
				}
				else if ( getFullTextFrom.equalsIgnoreCase("xml header") ) {
					fulltextFromXmlHeader.add(au_id);
				}
				else if ( getFullTextFrom.equalsIgnoreCase("no fulltext") ) {
					nofulltext.add(au_id);
				}
			}
			
		}


		processedContentSetIssueList.add(issueId);
	}

	
	/**
	 * MUSE has 32,264 AUs that has pub year < 1930, which are wrong. 
	 * Example: ark:/27927/phw55d5tvb	MUSE	ISSN_00083755	v.27	n.2  C:\workspace_neon\TDM-pilot\input\muse\ISSN_00083755\phw55d5tvb\data\phw55jvd8c.xml
     * a_au.a_publication_year=-1, a_au_dmd.pmd_date='05 April 17', a_publication_year=-1, a_cu.a_publication_date='170405';
	 * To fix this, use a_cu.a_publication_date and add '20' in front of '170405'. Update all articles and issue's pub date, pub_year in Analytics DB and return issue/article with fixed date..
	 * @param publisherID
	 * @param issue
	 */
	private DWIssue fixIssuePubDate(String publisherID, DWIssue issue) {
		if ( !publisherID.equals("MUSE")) {
			return issue;
		}
		
		String csname = issue.getContent_set_name();
		String volNo = issue.getVol_no();
		String issueNo = issue.getIssue_no();
		
		List<DWArticle> articles = issue.getArticles();
		
		
		String query = "select au.pmd_object_id au_id, dmd.a_publication_year dmd_year, au.a_publication_year au_year, cu.a_publication_date cu_date,   dmd.pmd_date,   cu.a_publication_date as cu_publication_date  "
						+ " from a_au_dmd dmd inner join a_au au on dmd.a_au_id=au.pmd_object_id "
						+ " and au.pmd_content_set_name='" + csname + "' inner join a_cu cu on au.pmd_object_id=cu.pmd_au_id "
						+ " and dmd.a_volume=q'$" + volNo + "$' "
					//	+ " and dmd.a_issue =q'$" + issueNo + "$' "    //if issueNo = 'null', dmd.a_issue is null
						+ " and au.pmd_object_id=?";	
		String issue_update = "update dw_issue set pub_year=?, pub_date=? where issn_no=? and vol_no=? and issue_no=?";
		String article_update = "update dw_article pub_year=?, pub_date=? where issn_no=? and vol_no=? and issue_no=? and au_id=?";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				PreparedStatement stmt = conn.prepareStatement(query); 
				PreparedStatement stmt1 = conn.prepareStatement(issue_update);
				PreparedStatement stmt2 = conn.prepareStatement(article_update); ) {
			
			String issue_new_year = null;
			
			for(DWArticle article: articles) {
				String au_id = article.getAu_id();
				String article_pub_year = article.getPub_year();
				
				stmt.setString(1, au_id);
				ResultSet rs = stmt.executeQuery();
				
				String new_pub_year = null;
				String new_pub_date = null;
				
				if (rs.next()) {
					//String au_id = rs.getString("au_id");
					String dmd_year = rs.getString("dmd_year");
					String au_year = rs.getString("au_year");
					String cu_date = rs.getString("cu_date");
					String pmd_date = rs.getString("pmd_date");
					
					if ( dmd_year.equals("-1") && au_year.equals("-1") && pmd_date.matches("^\\d{2} \\D+ \\d{2}$") ) {		//05 April 17
						new_pub_date = "20" + cu_date;					//'170405' --> '20170405'
						new_pub_year = new_pub_date.substring(0, 4);	//'2017'
						
						if ( new_pub_year != article_pub_year) {
							article.setPub_year(new_pub_year);
							SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
							Date pub_date = sdf1.parse(new_pub_date);
							
							article.setPub_date(pub_date);
							
							try {
								article.updateDateInDB( conn );
							}
							catch(Exception e) {
								logger.error( programName + ":fixIssuePubDate Error updating article dates in DB " + e );
							}
							
							if ( issue_new_year == null ) {
								issue_new_year = new_pub_year;
								issue.setPub_year(new_pub_year);
								issue.setPub_date(pub_date);
							}
						}
					}
				}
				else {
					logger.error( programName + ":fixIssuePubDate Error getting issue data metadata " + au_id + " " + query);
					continue;
				}

				rs.close();
			}
			
			//update issue in DB
			issue.updateDateInDB(conn);
			
			//TODO: check if articles in issue has been updated
			
		}
		catch(Exception e) {
			logger.error( programName + ":fixIssuePubDate  " + csname + " " + volNo + " " + issueNo + " " + " " + e);
		}
		
		
		
		return issue;
	}

	/**
	 * This is designed for SAGE, which has 2 content set names for ebook content
	 * @param publisher
	 * @param cs
	 * @throws Exception 
	 */
	public void processV2BookOfAContentSet(String publisher, String cs) throws Exception {
		
		List<String> bookAus = null;
		String publisherBookCSName = null;
		
		Connection conn = null;
		try {
			conn = TDMUtil.getConnection("PROD");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			publisherBookCSName = cs;
			bookAus = TDMUtil.getDedupedBookListForAPublisher(conn, publisher, publisherBookCSName);
		}
		catch(Exception e) {
			logger.error( programName + ":processV2BookOfAContentSet for " + publisher + ". " + e.getMessage());
			throw e;
		}
		finally {
			conn.close();
		}
		
		logger.info( programName + ":processV2BookOfAContentSet Total of " + (bookAus.size() + 1 ) + " books of " + publisherBookCSName + " to be processed ........" );
		
		boolean hasOA = false;
		if ( TDMUtil.getOAStatusOfBookContentSet(cs)) {
			hasOA = true;
		}
		
		int i=1;
		for( String bookAU: bookAus ) { 
			try {
				
				processV2ABookAU( publisher, bookAU, hasOA, i++ );
				
			}
			catch(Exception e) {
				logger.error( programName + ":processV2BookOfAContentSet :processV2ABookAU " + bookAU + ". " + e.getMessage());
				e.printStackTrace();
			}

		}
		
	}

	/**
	 * This method picks deduped ebooks of a publisher from ana_unified_book table
	 * and generate v2 files for the selected books.
	 * @param publisher
	 * @throws Exception 
	 */
	private void processV2BooksOfPublisher(String publisher) throws Exception {
		
		List<String> bookAus = null;
		String publisherBookCSName = null;
		
		Connection conn = null;
		try {
			conn = TDMUtil.getConnection("PROD");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			publisherBookCSName = TDMUtil.findPublisherBookContentSetName( publisher );
			bookAus = TDMUtil.getDedupedBookListForAPublisher(conn, publisher, publisherBookCSName);
		}
		catch(Exception e) {
			logger.error( programName + ":processV2BooksOfPublisher for " + publisher + ". " + e.getMessage());
			throw e;
		}
		finally {
			conn.close();
		}
		
		logger.info( programName + ":processV2BooksOfPublisher Total of " + (bookAus.size() + 1 ) + " books of " + publisherBookCSName + " to be processed ........" );
		
		boolean hasOA = false;
		if ( TDMUtil.getOAStatusOfBookContentSet( publisherBookCSName ) ) {
			hasOA = true;
		}
		
		int i=1;
		for( String bookAU: bookAus ) { 
			try {
				
				processV2ABookAU( publisher, bookAU, hasOA, i++ );
				
			}
			catch(Exception e) {
				logger.error( programName + ":processV2BooksOfPublisher:processV2ABookAU " + bookAU + ". " + e.getMessage());
				e.printStackTrace();
			}

		}
		
	}


	private void processV2BookAUsInAFile(String publisher, String filename, boolean hasOABookPublisher) {
		
		List<String> book_aus = null;
		
		try {
			book_aus = TDMUtil.getListFromFile(filename);
		}
		catch(Exception e) {
			logger.error( programName + "processV2BookAUsInAFile:getListFromFile " + filename + ". " + e.getMessage());
			return;
		}
		
		logger.info( programName + ":processV2BookAUsInAFile Total of " + (book_aus.size() + 1 ) + " books in " + filename + " to be processed ........" );
		
		int i= 1;
		for( String auid: book_aus ) { 
			
			try {
				processV2ABookAU( publisher, auid, hasOABookPublisher, i++ );
			}
			catch(Exception e) {
				logger.error( programName + ":processV2BookAUsInAFile:processV2ABookAU for " + publisher + " " + auid + ". " + e.getMessage());
			}

		}
		
	}



	public void processV2ABookAU(String publisherID, String auid, boolean hasOA, int index) throws Exception {
		
		logger.info(programName + ":processV2ABookAU process book " + index + ": " + auid );
		//System.out.println("-------> process book (" + index + ") " + auid);
		
		String publisherBookCSName = TDMUtil.findPublisherBookContentSetName( publisherID, auid );
		if ( publisherBookCSName == null ) {
			logger.error(programName + ":processV2ABookAU: No ebook content set name found for publisher " + publisherID);
			throw new Exception("No content set name found");
		}

		
		V2Book v2book = null;
		try {
			v2book = new V2Book(auid);
			
		} catch (Exception e1) {
			logger.error( programName + ":processV2ABookAU: Error createing new V2Book with " + auid + " " + e1.getMessage());
			throw e1;
		}
		
		v2book.setInputDir( inputDir + File.separator + "ebook");
		v2book.setOutputDir( outputDir + File.separator + "ebook");
		v2book.setSubDir(getSubDir());
		v2book.setPublisherId(publisherID);
		v2book.setContentSetName(publisherBookCSName);
		v2book.setBookOutputFormat();
		v2book.setFullTextAvailable(hasOA);
		
		try {
			//populate V2Book and its V2Chapters
			v2book.populatePorticoBookMetadata();
		} catch (Exception e) {
			
			if (e.getMessage().equalsIgnoreCase("Pub year 0")) {
				logger.error( programName + ":processV2ABookAU: populatePorticoBookMetadata " + auid + " " + e.getMessage() + ". Stop processing.");
				throw e;
			}
			
			e.printStackTrace();
			logger.error( programName + ":processV2ABookAU: populatePorticoBookMetadata " + auid + " " + e.getMessage());
		}
		
		try {
			//get fulltext and word frequency for V2Book and its V2Chapters
			v2book.performBookNLPTask();   
		} catch (Exception e1) {
			e1.printStackTrace();
			logger.error( programName + ":processV2ABookAU: performBookNLPTask " + auid + " " + e1);
			throw e1;
		}

		//log V2Book and its V2Chapters to TDM_BOOK table
		try {
			v2book.logToDB();
		} catch (Exception e) {
			logger.error( programName + ":processV2ABookAU: logToDB: " + publisherID + " " + auid + e);
			e.printStackTrace();
			throw e;
		}

		try {
			//Write out V2Book's json file and V2Chapters' json files
			v2book.outputJson();
		} catch (IOException e) {
			logger.error( programName + ":processV2ABookAU :outputJson: " + publisherID + " " + auid + e);
			e.printStackTrace();
			throw e;
		}

		processedArticleOrBookList.add(auid);

		String getFullTextFrom = v2book.getFulltextFrom();

		if ( getFullTextFrom != null ) {
			if ( getFullTextFrom.equalsIgnoreCase("pdf") ) {
				fulltextFromPdf.add(auid);
			}
			else if (getFullTextFrom.equalsIgnoreCase("xml")  ) {
				fulltextFromXml.add(auid);
			}
			else if ( getFullTextFrom.equalsIgnoreCase("html") ) {
				fulltextFromHTML.add(auid);
			}
			else if ( getFullTextFrom.equalsIgnoreCase("xml header") ) {
				fulltextFromXmlHeader.add(auid);
			}
			else if ( getFullTextFrom.equalsIgnoreCase("no fulltext") ) {
				nofulltext.add(auid);
			}
		}

		processedChapterCount += v2book.getProcessedChapterCount();
		
	}

	private static Options constructOptions() {

		final Options options = new Options();

		options.addOption(Option.builder("auid").required(false).hasArg().desc("Create json file for one AU").build() );
		options.addOption(Option.builder("cs").required(false).hasArg().desc("Create json file for all AUs for a content set").build() );
		options.addOption(Option.builder("v").required(false).hasArg().desc("Create json file for a content set issue. Use with cs and n option").build() );
		options.addOption(Option.builder("n").required(false).hasArg().desc("Create json file for a content set issue. Use with cs and v option").build() );
		options.addOption(Option.builder("csfile").required(false).hasArg().desc("Create json file for all content sets in a file, one per line").build() );
		options.addOption(Option.builder("issuefile").required(false).hasArg().desc("Create json file for issues supplied in the file").build() );
		options.addOption(Option.builder("subdir").required(true).hasArg().desc("The subdirectory name under input directory where AU files of Content Set have been stored.").build());
		options.addOption(Option.builder("publisher").required(false).hasArg().desc("Create json file for a publisher").build() );
		options.addOption(Option.builder("scandir").required(false).desc("Process AUs under CS dir").build() );
		options.addOption(Option.builder("newcontent").required(false).desc("Process AUs from input/newcontent_yyyyMM and output json files to output/newcontent_yyyyMM").build() );
		
		options.addOption(Option.builder("aufile").required(false).hasArg().desc("Lists AUs of ebooks").build() );
		options.addOption(Option.builder("book").required(false).desc("Indicate generate jons files for books").build() );
		
		return options;
	}

	
	private static void printUsage(Options options) {
		
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "PorticoV2Processor", options);  
		writer.flush();  
		
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

	public boolean isJournalType() {
		return journalType;
	}

	public void setJournalType(boolean journalType) {
		this.journalType = journalType;
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

	public boolean isScandir() {
		return scandir;
	}

	public void setScandir(boolean scandir) {
		this.scandir = scandir;
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

	public String getCacheDir() {
		return cacheDir;
	}

	public void setCacheDir(String cacheDir) {
		this.cacheDir = cacheDir;
	}

	public List<String> getProcessedContentSetList() {
		return processedContentSetList;
	}

	public void setProcessedContentSetList(List<String> processedContentSetList) {
		this.processedContentSetList = processedContentSetList;
	}

	public List<String> getProcessedContentSetIssueList() {
		return processedContentSetIssueList;
	}

	public void setProcessedContentSetIssueList(List<String> processedContentSetIssueList) {
		this.processedContentSetIssueList = processedContentSetIssueList;
	}

	public List<String> getProcessedArticleOrBookList() {
		return processedArticleOrBookList;
	}

	public void setProcessedArticleOrBookList(List<String> processedArticleOrBookList) {
		this.processedArticleOrBookList = processedArticleOrBookList;
	}

	public int getProcessedChapterCount() {
		return processedChapterCount;
	}

	public void setProcessedChapterCount(int processedChapterCount) {
		this.processedChapterCount = processedChapterCount;
	}

	public int getProcessedAUCount() {
		return processedAUCount;
	}

	public void setProcessedAUCount(int processedAUCount) {
		this.processedAUCount = processedAUCount;
	}

	public int getSkippedAUCount() {
		return skippedAUCount;
	}

	public void setSkippedAUCount(int skippedAUCount) {
		this.skippedAUCount = skippedAUCount;
	}



}

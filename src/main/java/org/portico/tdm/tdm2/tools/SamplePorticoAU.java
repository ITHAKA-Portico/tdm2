package org.portico.tdm.tdm2.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import javax.xml.parsers.ParserConfigurationException;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This class is used to get information from sampled Portico AU files.
 * The sample part is done by Oracle query "sample(%)". AU_IDs been exported. Use exportAus.sh to extract these AUs' data to server disk.
 * This program reads xml file from server disk, and saves data to tdm_au table. 
 * 
 * Task1, sample xml file from 1 million Portico AUs, get @article-type, store in tdm_au.article_type field.
 * 
 * Usage:
 * 	java org.portico.tdm.tools.SamplePorticoAU [-aufile tdm_au_aa] [-au ark:/27927/pc013dgtdb]
 *  nohup java org.portico.tdm.tools.SamplePorticoAU -aufile tdm_au_8.tsv > /dev/null 2>&1 &
 * 
 * @author dxie
 *
 */
public class SamplePorticoAU {
	
	static Properties props = new Properties();
	static Logger logger = LogManager.getLogger(SamplePorticoAU.class.getName());
	static String programName = "SamplePorticoAU";
	
	String configDir = "config";
	String inputDir = "input";
	String subDir = "sampledata";
	String server = "PROD";
	String noArticleType = "N/A";
	
	long starttime, endtime;
	String startTimeStr, endTimeStr;
	String argStr;
	
	int totalAUCount;
	List<String> notExistAUs;
	Set<String> processedContentSets;
	Set<String> processedPublishers;
	Map<String, MutableInt> articleTypeValueMap;		//article-type --> count of article-type
	Map<String, MutableInt> publisherArticleTypeMap;	//publisher-> count of article-type used
	
	class MutableInt {
		int value = 1; 				// note that we start at 1 since we're counting
		public void increment () 	{ ++value;      }
		public int  get ()       	{ return value; }
	}
	
	public SamplePorticoAU() {
		//get startTime
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		startTimeStr = dateFormat.format(cal.getTime());
		starttime = System.currentTimeMillis();
		
		totalAUCount = 0;
		notExistAUs = new ArrayList<>();
		articleTypeValueMap = new HashMap<String, MutableInt>();
		publisherArticleTypeMap = new HashMap<String, MutableInt>();
		
		processedPublishers = new HashSet<>();
		
	}

	public static void main(String[] args) {
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();
		
		try {
			CommandLine line = parser.parse( options, args );
			SamplePorticoAU sampler = new SamplePorticoAU();
			
			logger.info( programName + " with args: " + String.join(" ", args));
			sampler.setArgStr( String.join(" ", args)) ;
			
			if ( line.hasOption("aufile")) {
				String filename = line.getOptionValue("aufile");

				sampler.sampleAUFromFile( filename );
			}
			else if ( line.hasOption("au")) {
				String auid = line.getOptionValue("au");
				
				sampler.sampleOneAU( auid, 1 );
			}
			else {
				SamplePorticoAU.printUsage( options );
			}
			
			sampler.printStats();
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
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
        logger.info("Successfully processed AUs (" + totalAUCount + "): " );
        logger.info("Not exist AU count = " + notExistAUs.size());
        for( String auid: notExistAUs ) {
        	logger.info( "\t" + auid);
        }
        
        logger.info("Count of @article-type used in these AUs' xml files");
        for (String articleType: articleTypeValueMap.keySet()){
            MutableInt count = articleTypeValueMap.get(articleType);  
            logger.info( "\tArticle-type: " + articleType + "\t\t\t" + count.value );  
        } 
        
        logger.info("Publishers used @article-type: ");
        for (String publisher: publisherArticleTypeMap.keySet()){
            MutableInt count = publisherArticleTypeMap.get(publisher);  
            logger.info( "\tPublisher : " + publisher + "\t\t\t" + count.value );  
        } 
        
        logger.info("Total process time " + startTimeStr + "[start] to " + endTimeStr + "[end], in total = " + timeUsedStr );
        logger.info("================================================================");
        
        
		
	}

	private void sampleAUFromFile(String filename) {
		
		logger.info( programName + ":sampleAUFromFile " + filename);
		List<String> AUlist = null;
		String inputFileDir = getConfigDir() ;
		String inputFile = inputFileDir + File.separator + filename;
		try {
			AUlist = TDMUtil.readAUListFromFile(inputFile);
		} catch (IOException e) {
			logger.error( programName + ":sampleAUFromFile " + filename + " " + e.getMessage());
			e.printStackTrace();
		}
		
		logger.info( programName + ":sampleAUFromFile : Read in " + AUlist.size() + " AUs ...");
		int i =1;
		for(String auid: AUlist) {
			try {
				sampleOneAU( auid, i++ );
			}
			catch(Exception e) {
				logger.error( programName + ":sampleAUFromFile: " + auid + " " + e.getMessage());
			}
		}
		
	}

	/**
	 * Open XML file, read in @article-type, fill tdm_au.article-type
	 * @param auid ie ark:/27927/pc013dgtdb
	 * @param i
	 * @throws Exception 
	 */
	private void sampleOneAU(String auid, int i) throws Exception {
		
		totalAUCount ++;
		
		String subdir = getSubDir();
		String inputdir = getInputDir();
		
		String bitsXmlFileName = null;
		try {
			bitsXmlFileName = TDMUtil.getBitsXmlFileNameForAU(auid);
		} catch (Exception e) {
			logger.error( programName + ":sampleOneAU: Error getting BITS xml file for " + auid);
			e.printStackTrace();
			throw e;
		}
		
		if ( bitsXmlFileName == null ) {
			notExistAUs.add(auid);
			updateTDMAUArticleType( auid, "Not exist");
			return;
		}
		
		//constructs full xml file name
		String fullBitsXmlFileName = inputdir + File.separatorChar + subdir +  File.separator + auid.replace("ark:/27927/", "") 
					+ File.separator + "data" + File.separator + bitsXmlFileName;

		//parse xml file
		Document doc;
		try {
			doc = TDMUtil.parseXML( new File(fullBitsXmlFileName) );
		} catch (ParserConfigurationException | SAXException | IOException e) {
			throw new Exception( fullBitsXmlFileName + " cannot be parsed. " );
		}

		Element root = doc.getDocumentElement();
		//System.out.println(root.getNodeName());
		
		String article_type = null;
		
		
		
		NodeList nodeList = doc.getElementsByTagName("article");
		try {
			for(int x=0,size= nodeList.getLength(); x<size; x++) {
				article_type = nodeList.item(x).getAttributes().getNamedItem("article-type").getNodeValue();
			}
		}
		catch(Exception e) {
			article_type = "null";
			logger.error(programName + ":sampleOneAU" + auid + " null article-id attribute" );
		}
        
        
        //update tdm_au.article_type
        updateTDMAUArticleType( auid, article_type);
        
     
        //change counters
        String publisherID = TDMUtil.getPublisherFromAU(auid);

        if ( article_type == null || article_type.equals("null")) {		//@article-type not set in xml file
        	article_type = "null";
        }
        else {
        	//add publisherArticleTypeMap counters
        	MutableInt count = publisherArticleTypeMap.get(publisherID);
        	if (count == null) {
        		publisherArticleTypeMap.put(publisherID, new MutableInt());
            }
            else {
                count.increment();
            }
       	
        }
		
        MutableInt count = articleTypeValueMap.get(article_type);
        if (count == null) {
        	articleTypeValueMap.put(article_type, new MutableInt());
        }
        else {
            count.increment();
        }
        
        logger.info( i + ", AU:" + auid + " XML:" + bitsXmlFileName + " article-type:" + article_type);
        
	}


	private void updateTDMAUArticleType(String auid, String article_type) throws Exception {
		
		String query = "update tdm_au set article_type='" + article_type + "' where au_id='" + auid + "'";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			stmt.execute(query);
			
		}
		catch(Exception e) {
			logger.error( programName + ":updateTDMAUArticleType " + e.getMessage());
			throw e;
		}
		
	}

	/**
	 * If element is not null, get the first matched element's (name is tag) text value.
	 * @param e
	 * @param tag
	 * @return
	 */
	private String getSubtreeElementValueByTagName(Element e, String tag) {
		String value = null;
		
		NodeList nodes = e.getElementsByTagName(tag);		//Returns a NodeList of all descendant Elements with a given tag name, in document order.
		if ( nodes.getLength()> 0 ) {
			value = nodes.item(0).getTextContent().trim();
		}

		return value;
	}



	private List<String> readAUListFromFile(String filename) throws IOException {
		List<String> lists = new ArrayList<>();
		
		//read AUs from file
		String inputFileDir = getConfigDir() ;
		String inputFile = inputFileDir + File.separator + filename;

		//check file permission etc.
		File file = new File(inputFile);
		if ( ! file.exists()) {
			throw new FileNotFoundException("The input file " + inputFile + " cannot be found.");
		}

		if ( !file.canRead()) {
			throw new SecurityException("The input file " + inputFile + " cannot be read.");
		}

		//open and read from file
		logger.info("++++++++++++++++++++++++++++++++  " + inputFile + "  +++++++++++++++++++++++");
		logger.info("Opening file [" + file.getName() + "]");


		BufferedReader readbuffer = new BufferedReader(new FileReader( file));
		String line;

		int lineCount = 0;
		

		while ( (line = readbuffer.readLine()) != null) {
			if ( !line.trim().isEmpty()) {
				lists.add(line.trim());
				
				++lineCount;
			}
		}	
		readbuffer.close();
		System.out.println("Total lines in " + inputFile + " is " + lineCount );	
		
		return lists;
	}
	
	
	

	private static Options constructOptions() {

		final Options options = new Options();
	
		options.addOption(Option.builder("aufile").required(false).hasArg().desc("Lists AUs to sample").build() );
		options.addOption(Option.builder("au").required(false).hasArg().desc("One AU to sample").build() );
		
		return options;
	}
	
	private static void printUsage(Options options) {
		
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "SamplePorticoAU", options);  
		writer.flush();  
		
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

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getNoArticleType() {
		return noArticleType;
	}

	public void setNoArticleType(String noArticleType) {
		this.noArticleType = noArticleType;
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



	public Map<String, MutableInt> getArticleTypeValueMap() {
		return articleTypeValueMap;
	}

	public void setArticleTypeValueMap(Map<String, MutableInt> articleTypeValueMap) {
		this.articleTypeValueMap = articleTypeValueMap;
	}

	public Map<String, MutableInt> getPublisherArticleTypeMap() {
		return publisherArticleTypeMap;
	}

	public void setPublisherArticleTypeMap(Map<String, MutableInt> publisherArticleTypeMap) {
		this.publisherArticleTypeMap = publisherArticleTypeMap;
	}

	public int getTotalAUCount() {
		return totalAUCount;
	}

	public void setTotalAUCount(int totalAUCount) {
		this.totalAUCount = totalAUCount;
	}

	public List<String> getNotExistAUs() {
		return notExistAUs;
	}

	public void setNotExistAUs(List<String> notExistAUs) {
		this.notExistAUs = notExistAUs;
	}

	public String getConfigDir() {
		return configDir;
	}

	public void setConfigDir(String configDir) {
		this.configDir = configDir;
	}


}

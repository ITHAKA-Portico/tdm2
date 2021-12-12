package org.portico.tdm.tdm2.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



/**
 * This class is used to read in dataset json files and pull POS tags and populate them to TDM_POS table.
 * Usage: 
 * 	java org.portico.tdm.tools.JsonPOS2DB -publisher ACM
 * 	java org.portico.tdm.tools.JsonPOS2DB -publisher ACM -cs ISSN_01635697
 * 	java org.portico.tdm.tools.JsonPOS2DB -effilename output\acm\ISSN_01635697_v1_n2_EF.json
 * 	java org.portico.tdm.tools.JsonPOS2DB -datasetfilename input\sampledata\mdp.39015004629666_features.json
 * 	java org.portico.tdm.tools.JsonPOS2DB -dsidlistfile 
 * 	java org.portico.tdm.tools.JsonPOS2DB -datasetdir input\sampledata\
 * 	java org.portico.tdm.tools.JsonPOS2DB -datasetsintxt  data\portico\portico.txt 
 * 
 * @author dxie
 *
 */
public class JsonPOS2DB {
	
	
	static String[] acceptedTokenTags = {

		    //"CC", // coordinating conjunction
		    //"CD", // cardinal digit
		    //"DT", // determiner
		    "EX", // existential there (like: "there is" ... think of it like "there exists")
		    "FW", // foreign word
		    //"IN", // preposition/subordinating conjunction
		    "JJ", // adjective 'big'
		    "JJR", // adjective, comparative 'bigger'
		    "JJS", // adjective, superlative 'biggest'
		    //"LS", //", // list marker 1)
		    "MD", // modal could, will
		    "NN", // noun, singular 'desk'
		    "NNS", // noun plural 'desks'
		    "NNP", // proper noun, singular 'Harrison'
		    "NNPS", // proper noun, plural 'Americans'
		    "PDT", // predeterminer 'all the kids'
		    "POS", // possessive ending parent's
		    "PRP", // personal pronoun I, he, she
		    "PRP$", // possessive pronoun my, his, hers
		    "RB", // adverb very, silently,
		    "RBR", // adverb, comparative better
		    "RBS", // adverb, superlative best
		    "RP", // particle give up
		    //"TO", // to go 'to' the store.
		    "UH", // interjection errrrrrrrm
		    "VB", // verb, base form take
		    "VBD", // verb, past tense took
		    "VBG", // verb, gerund/present participle taking
		    "VBN", // verb, past participle taken
		    "VBP", // verb, sing. present, non-3d take
		    "VBZ", // verb, 3rd person sing. present takes
		    //"WDT", // wh-determiner which
		    //"WP", // wh-pronoun who, what
		    //"WP$", // possessive wh-pronoun whose
		    //"WRB" // wh-abverb where, when
	};
	List<String> acceptedTokenTagsList ;
	
	static Properties props = new Properties();
	static Logger logger = LogManager.getLogger(JsonPOS2DB.class.getName());
	static String programName = "JsonPOS2DB";
	
	Set<String> token_sets = new HashSet<>();
	Map<String, Map<String, Integer>> doc_token_maps = new HashMap<>();
	
	int processedFileOrObjectCount = 0;
	int processedTokenCount = 0;

	public JsonPOS2DB() {
		acceptedTokenTagsList = new ArrayList<>();
		
		for(String tag: acceptedTokenTags) {
			acceptedTokenTagsList.add(tag);
		}
	}
	
	
	public static void main(String[] args) {
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();

		JsonPOS2DB worker = new JsonPOS2DB(  );

		// parse the command line arguments
		try {
			CommandLine line = parser.parse( options, args );
			String publisher;
			String cs;

			if ( line.hasOption("publisher")) {
				publisher = line.getOptionValue("publisher");
				
				if ( line.hasOption("cs")) {
					cs = line.getOptionValue("cs");

					worker.processOneContentSetJsonEF( publisher, cs );
				}
				else {

					worker.processAllJsonEFForAPublisher( publisher );
				}

			}
			else if ( line.hasOption("effilename")) {
				String filename = line.getOptionValue("effilename");

				worker.processOneJsonEF( filename );


			}
			else if ( line.hasOption("datasetfilename")) {
				String filename = line.getOptionValue("datasetfilename");

				worker.processOneJsonDataset( filename );


			}
			else if ( line.hasOption("dsidlistfile")) {
				String filename = line.getOptionValue("dsidlistfile");

				worker.processJsonDatasetInAFile( filename );


			}
			else if ( line.hasOption("datasetdir")) {
				String dir = line.getOptionValue("datasetdir");
				
				worker.processAllDatasetsInADirectory( dir );
			}
			else if ( line.hasOption("datasetsintxt")) {
				String txtfile = line.getOptionValue("datasetsintxt");
				
				worker.processAllDatasetsInATxtFile( txtfile );
			}
			
			else {
				JsonPOS2DB.printUsage( options );
			}


		}
		catch(Exception e) {

		}


	}
	

	/**
	 * Process a file that contains multiple dataset json objects. Each line is a dataset json object.
	 * @param txtfile
	 * @throws Exception
	 */
	private void processAllDatasetsInATxtFile(String txtfile) throws Exception {
		
		logger.info( programName + ":processAllDatasetsInATxtFile: process dataset json objects in file " + txtfile );


	    JSONObject obj = null;

	    // This will reference one line at a time
	    String line = null;

	    try {
	        // FileReader reads text files in the default encoding.
	        FileReader fileReader = new FileReader(txtfile);

	        // Always wrap FileReader in BufferedReader.
	        BufferedReader bufferedReader = new BufferedReader(fileReader);

	        while((line = bufferedReader.readLine()) != null) {
	        	
	        	String id = null;
	        	try {
					obj = (JSONObject) new JSONParser().parse(line);
					id = (String)obj.get("id");
				} catch (ParseException e) {
					logger.error( programName + ":processAllDatasetsInATxtFile: Error parsing a line to JSONObject " +  e.getMessage());
				}

	            //System.out.println((String)obj.get("id")+":"+(String)obj.get("type"));
	        	try {
					processDatasetInAString( line, id );
				} catch (Exception e) {
					logger.error( programName + ":processAllDatasetsInATxtFile:  " +  e.getMessage());
					e.printStackTrace();
				}


	        }
	        // Always close files.
	        bufferedReader.close();         
	    }
	    catch(Exception ex) {
	        throw new Exception("Error read file '" + txtfile + "'");                
	    }
	    
		logger.info( programName + ":processAllDatasetsInATxtFile: total dataset json files have been processed=" + processedFileOrObjectCount 
				+ ", total token count = " + processedTokenCount );


	    
		
	}


	/**
	 * Process on dataset json object in one string.
	 * Global variable processedFileOrObjectCount and processedTokenCount will be updated.
	 * @param jsonDatasetStr
	 * @param id
	 * @return Token count have been processed
	 * @throws Exception
	 */
	private int processDatasetInAString(String jsonDatasetStr, String id) throws Exception {
		

		logger.info( programName + ":processDatasetInAString for " + id );
		
		//create ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();

		//read JSON like DOM Parser
		JsonNode rootNode = null;
		try {
			rootNode = objectMapper.readTree(jsonDatasetStr);
		} catch (IOException e) {
			logger.error( programName + ":processDatasetInAString: Error mapping a string to JsonNode " + id + " " + e.getMessage());
			throw e;
		}

		JsonNode idNode = rootNode.path("id");
		String doc_id = idNode.asText();
		
		if ( doc_id.startsWith("portico-") || doc_id.startsWith("jstor-")) {
			doc_id = doc_id.replace("portico-", "").replace("jstor-", "");
			doc_id = doc_id.replaceAll("-\\d{8}$", "");
		}
		
		String pubYear = rootNode.path("metadata").path("pubDate").asText();
		
		int tokenCount = processPOSInJsonNode( doc_id, pubYear, rootNode);

		//write to DB
		try {
			outputPOS2DB( doc_id, pubYear );
		} catch (Exception e) {
			logger.error( programName + ":processDatasetInAString:  " + doc_id + " " + e.getMessage());
			throw e;
		}
		
		processedTokenCount += tokenCount;
		processedFileOrObjectCount += 1;
		
		logger.info( programName + ":processDatasetInAString: total token count = " + tokenCount );

		
		return tokenCount;
	}


	/**
	 * Process all dataset json files under one directory
	 * @param dirName
	 */
	private void processAllDatasetsInADirectory(String dirName) {
		
		File[] files = new File( dirName ).listFiles(
				new FilenameFilter() { @Override public boolean accept(File dir, String name) 
				      { return name.endsWith(".json"); } });
		
		logger.info( programName + ":processAllDatasetsInADirectory: total dataset json files under directory = " + files.length);
		
		
		for(File file: files) {
			String filename = file.getName();
			try {
				processOneJsonDataset( dirName + File.separator + filename );
			}
			catch(Exception e) {
				logger.error(programName + ":processAllDatasetsInADirectory: Error processing one dataset file " + filename + " " +e.getMessage());
			}
			
		}
		
		logger.info( programName + ":processAllDatasetsInADirectory: total dataset json files have been processed=" + processedFileOrObjectCount 
				+ ", total token count = " + processedTokenCount );

		
	}


	/**
	 * Portico only. Process all ef json files for a publisher, which are stored at certain location.
	 * @param publisher
	 * @throws Exception
	 */
	private void processAllJsonEFForAPublisher(String publisher) throws Exception {
		

		
		List<String> contentSets = null;
		try {
			contentSets = TDMUtil.getTDMContentSetsOfPublisher( publisher );
		} catch (Exception e) {
			logger.error( programName + ":processAllJsonEFForAPublisher:getTDMContentSetsOfPublisher " + publisher + " " + e.getMessage());
			throw e;
		}
		
		logger.info( programName + ":processAllJsonEFForAPublisher: total content set counts = " + contentSets.size() + " for " + publisher );

		
		for(String csname: contentSets) {
			try {
				processOneContentSetJsonEF(publisher, csname);
			} catch (Exception e) {
				logger.error( programName + ":processAllJsonEFForAPublisher:processOneContentSetJsonEF " + publisher + " " + csname + " " + e.getMessage());
			}
		}
		
		logger.info( programName + ":processAllJsonEFForAPublisher: total content sets have been processed = " + contentSets.size() + 
				", total extracted feature json files have been processed=" + processedFileOrObjectCount + ", total token count = " + processedTokenCount );
		
	}

	/**
	 * Portico only. Get features data from _EF.json files and pub_year from querying dw_issue tables.
	 * Global variable processedTokenCount and processedFileCount will be updated.
	 * @param cs
	 * @throws Exception 
	 */
	private void processOneContentSetJsonEF(String publisher_id, String cs) throws Exception {
		
		logger.info( programName + ":processOneContentSetJsonEF: process content set " + cs + " of " + publisher_id );

		
		String pub_dir = "output" + File.separator;
		if ( publisher_id.equalsIgnoreCase("CAMBRIDGE")) {
			pub_dir += "camb" ;
		}
		else {
			pub_dir += publisher_id.toLowerCase() ;
		}

		List<String> issueIDs = null;
		
		try {
			issueIDs = TDMUtil.getIssueIDsForAContentSet( cs );  //ISSN_07115075_v1_n1
		} catch (Exception e) {
			logger.error(programName + ":processOneContentSetJsonEF: Error getIssueIDsForAContentSet  " + cs + " " +e.getMessage());
			throw e;
		}
		
		int csTokenCount = 0;
		
		for(String issue_id: issueIDs ) {   
			String jsonEFFilename = pub_dir + File.separator + issue_id.replaceAll("\\s", "").replace("\u2010", "-").replace("\u2011", "-").replace("\u2013", "-") + "_EF.json";
			
			try {
				csTokenCount += processOneJsonEF(jsonEFFilename);
			} catch (Exception e) {
				logger.error(programName + ":processOneContentSetJsonEF: Error processOneJsonEF  " + jsonEFFilename + " " +e.getMessage());
			}
		}
		
		logger.info( programName + ":processOneContentSetJsonEF: total extracted feature json files have been processed=" + issueIDs.size() + ", total token count = " + csTokenCount );
		

		
	}

	

	/**
	 * Process one json ef file and save tokens&tags (body only) into TDM_POS table.
	 * The difference of this method vs processOneJsonDataset is that ef file doesn't have metadata
	 * content, pub_year need to be read from dw_issue table.
	 * For demo purpuse, the parser only read part of the tree, not using the object model.
	 * Global variable processedFileOrObjectCount and processedTokenCount will be updated.
	 * 
	 * @param filename The relative path file name, ie output/ISSN_00034746_v165_n2_EF.json
	 * @throws Exception 
	 */
	private int processOneJsonEF(String filename) throws Exception {
		
		logger.info( programName + ":processOneJsonEF: " + filename );
		
		//read json file data to String
		byte[] jsonData = null;
		try {
			jsonData = Files.readAllBytes(Paths.get(filename));
		} catch (IOException e) {
			logger.error( programName + ":processOneJsonEF: Error reading file " + filename + " " + e.getMessage());
			throw e;
		}

		//create ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();

		//read JSON like DOM Parser
		JsonNode rootNode = null;
		try {
			rootNode = objectMapper.readTree(jsonData);
		} catch (IOException e) {
			logger.error( programName + ":processOneJsonEF: Error reading json object in " + filename + " " + e.getMessage());
			throw e;
		}

		JsonNode idNode = rootNode.path("id");
		String doc_id = idNode.asText();
		
		if ( doc_id.startsWith("portico-")) {
			doc_id = doc_id.replace("portico-", "");
			doc_id = doc_id.replaceAll("-\\d{8}$", "");
		}
		
		String pubYear = "n/a";
		try {
			pubYear = TDMUtil.getPubYearForAIssue(doc_id);
		} catch (Exception e1) {
			logger.error( programName + ":processOneJsonEF: Error get pubYear for issue " + doc_id + " " + e1.getMessage());
		}

		
		int tokenProcessed = processPOSInJsonNode( doc_id, pubYear, rootNode);
		
		//write to DB
		try {
			outputPOS2DB( doc_id, pubYear );
		} catch (Exception e) {
			logger.error( programName + ":processOneJsonEF:  " + doc_id + " " + e.getMessage());
			throw e;
		}
		
		logger.info( programName + ":processOneJsonEF: total token count = " + tokenProcessed + " in " + filename);

		processedFileOrObjectCount++;
		processedTokenCount += tokenProcessed;
		
		return tokenProcessed;
		
	}


	/**
	 * Process one json dataset file and save tokens&tags (body only) into TDM_POS table
	 * For demo purpuse, the parser only read part of the tree, not using the object model.
	 * Global variable processedFileOrObjectCount and processedTokenCount will be updated.
	 * 
	 * @param filename The relative path file name, ie input/sampledata/text1.json
	 * @throws Exception 
	 * @return Token count have been processed
	 */
	private int processOneJsonDataset(String filename) throws Exception {
		
		logger.info( programName + ":processOneJsonDataset: " + filename );
		
		//read json file data to String
		byte[] jsonData = null;
		try {
			jsonData = Files.readAllBytes(Paths.get(filename));
		} catch (IOException e) {
			logger.error( programName + ":processOneJsonDataset: Error reading file " + filename + " " + e.getMessage());
			throw e;
		}

		//create ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();

		//read JSON like DOM Parser
		JsonNode rootNode = null;
		try {
			rootNode = objectMapper.readTree(jsonData);
		} catch (IOException e) {
			logger.error( programName + ":processOneJsonDataset: Error reading json object in " + filename + " " + e.getMessage());
			throw e;
		}

		JsonNode idNode = rootNode.path("id");
		String doc_id = idNode.asText();
		
		if ( doc_id.startsWith("portico-") || doc_id.startsWith("jstor-")) {
			doc_id = doc_id.replace("portico-", "").replace("jstor-", "");
			doc_id = doc_id.replaceAll("-\\d{8}$", "");
		}
		
		String pubYear = rootNode.path("metadata").path("pubDate").asText();
		
		int tokenCount = processPOSInJsonNode( doc_id, pubYear, rootNode);

		//write to DB
		try {
			outputPOS2DB( doc_id, pubYear );
		} catch (Exception e) {
			logger.error( programName + ":processOneJsonDataset:  " + doc_id + " " + e.getMessage());
			throw e;
		}
		

		logger.info( programName + ":processOneJsonDataset: total token count = " + tokenCount + " in " + filename);

		processedFileOrObjectCount++;
		processedTokenCount += tokenCount;
		
		
		return tokenCount;
		
	}

	/**
	 * Extract one document's features/pages/body/tokenPosCount, store wanted tokens in token_sets and toc_token_maps (reset for each document)
	 * @param doc_id
	 * @param pubYear
	 * @param rootNode
	 * @return Total token counts have been processed
	 */
	private int processPOSInJsonNode(String doc_id, String pubYear, JsonNode rootNode) {
		
		logger.info( programName + ":processPOSInJsonNode: id = "+ doc_id + ", pub year=" + pubYear );
		
		// initialize global variable for each doc
		token_sets = new HashSet<>(); 
		doc_token_maps = new HashMap<>();

		List<JsonNode> body_nodes = rootNode.path( "features").path("pages").findValues("body");

		for(JsonNode bodyNode: body_nodes) {
			List<JsonNode> token_nodes = bodyNode.findValues("tokenPosCount");

			for(JsonNode token_node: token_nodes ) {

				Iterator<String> iterator = token_node.fieldNames();

				while (iterator.hasNext()) {
					String tokenName = iterator.next();
					tokenName = tokenName.toLowerCase();
					JsonNode posNode = token_node.path(tokenName);

					Iterator<String> it2 = posNode.fieldNames();
					while( it2.hasNext()) {
						String tag = it2.next();
						JsonNode tagNode = posNode.path(tag);
						int count = tagNode.asInt();
						
						if ( tokenName.getBytes().length > 100 ) {	//unusual token
							logger.info( programName + ":processPOSInJsonNode: exclude too long token " + tokenName);
							continue;
						}

						if ( tag.startsWith("-")) {		//all these tokens can be excluded
							//logger.info( programName + ":processPOSInJsonNode: exclude token " + tokenName + " with tag " + tag);
							continue;
						}
						
						if ( token_sets.contains(tokenName)) {
							Map<String, Integer> count_map = doc_token_maps.get(tokenName);
							
							if ( count_map.keySet().contains(tag) ) {
								//add to existing count
								int old_count = count_map.get(tag).intValue();
								
								count_map.put(tag,  new Integer( old_count + count));
								
							}
							else {
								count_map.put(tag, new Integer(count));
								
							}
							doc_token_maps.put(tokenName, count_map);
						}
						else {
							if ( acceptedTokenTagsList.contains(tag)) {	//exclude some tag types
								//add this new token
								token_sets.add(tokenName.toLowerCase());
								Map<String, Integer> count_map = new HashMap<>();
								count_map.put(tag, new Integer(count));
								
								doc_token_maps.put(tokenName, count_map);
							}
							else {
								//logger.info( programName + ":processPOSInJsonNode: exclude token " + tokenName + ", tag=" + tag + ", count=" + count);
							}
						}
						
						//System.out.println(" token =" + tokenName + ", tag=" + tag + ", count=" + count);
					}
				}
			}
		}
		
		return token_sets.size();
		
	}


	/**
	 * This method writes the POS tags stored in doc_token_maps to TDM_POS table.
	 * All old tags in the document will be deleted first.
	 * @param doc_id
	 * @param pubYear
	 * @throws Exception 
	 */
	private void outputPOS2DB(String doc_id, String pubYear) throws Exception {

		
		//String query1 = "select count(doc_id) from tdm_pos where doc_id='" + doc_id + "'";
		String query2 = "delete tdm_pos where doc_id='" + doc_id + "'";
		String query3 = "insert into tdm_pos (doc_id, provider, pub_year, token, tag, count, type, create_timestamp ) "
						+ " values (?,?, ?,?,?,?,?,?) ";
		String provider = "";
		if ( doc_id.startsWith("ark:")) {
			provider = "Portico";
		}
		else if ( doc_id.matches("^i\\d+$") ) {
			provider = "JSTOR";
		}
		else {
			provider = "HTRC";
		}
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement statement = conn.createStatement(); 
				PreparedStatement prst = conn.prepareStatement(query3); ) {
			
			statement.executeUpdate(query2);
			
			for(String token: doc_token_maps.keySet()) {
				Map<String, Integer> count_map = doc_token_maps.get(token);
				for(String tag: count_map.keySet()) {
					int count = count_map.get(tag).intValue();
					
					prst.setString(1,  doc_id);
					prst.setString(2, provider);
					prst.setString(3, pubYear);
					prst.setString(4, token);
					prst.setString(5, tag);
					prst.setInt(6, count);
					prst.setString(7, "uni-gram");
					prst.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
					
					try {
						prst.execute();
					}
					catch(Exception e1) {
						logger.error( programName + ":outputPOS2DB: " + doc_id + " , token=" + token + ", tag=" + tag 
								+ ", count=" + count + " " + e1.getMessage());
					}
					
				}
			}
		}
		catch (Exception e) {
			throw new Exception("Error inserting POS tags for " + doc_id + " " + e.getMessage());
		}
		
	}


	/**
	 * Process dataset files listed in a file
	 * @param filename
	 * @throws Exception
	 */
	private void processJsonDatasetInAFile(String filename) throws Exception {
		
		List<String> doc_files = null;
		
		try {
			doc_files = TDMUtil.getListFromFile(filename);
		}
		catch(Exception e) {
			logger.error( programName + ":processJsonEFInAFile:getListsFromFile " + filename + ". " + e.getMessage());
			throw e;
		}
		
		logger.info( programName + ":processJsonDatasetInAFile: number of dataset files to be processed = " + doc_files.size());


		for( String doc_file: doc_files ) {
			
			try {
				processOneJsonDataset( doc_file );
			} catch (Exception e) {
				logger.error( programName + ":processJsonEFInAFile:Error processing one json dataset file " + doc_file + ". " + e.getMessage());
				e.printStackTrace();
				throw e;
			}
			
		}
		
		logger.info( programName + ":processJsonDatasetInAFile: total json dataset objects have been processed = " + processedFileOrObjectCount  
				 + ", total token count = " + processedTokenCount );

	}


	private static Options constructOptions() {

		final Options options = new Options();

		options.addOption(Option.builder("effilename").required(false).hasArg().desc("Read from one EF json file").build() );
		options.addOption(Option.builder("datasetfilename").required(false).hasArg().desc("Read from one dataset json file").build() );
		options.addOption(Option.builder("cs").required(false).hasArg().desc("Read all dataset files for a content set(Portico only)").build() );
		options.addOption(Option.builder("publisher").required(false).hasArg().desc("Read all dataset files for publisher(Portico only)").build() );
		options.addOption(Option.builder("listfile").required(false).hasArg().desc("A list of files").build() );
		options.addOption(Option.builder("dsidlistfile").required(false).hasArg().desc("A list of doc ids in a file").build() );
		options.addOption(Option.builder("datasetdir").required(false).hasArg().desc("Process all dataset json files under a directory").build() );
		options.addOption(Option.builder("datasetsintxt").required(false).hasArg().desc("Process all dataset json files concatinated in one txt file").build() );
		
		
		return options;
	}

	
	private static void printUsage(Options options) {
		
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "JsonPOS2DB", options);  
		writer.flush();  
		
	}



	public int getProcessedTokenCount() {
		return processedTokenCount;
	}


	public void setProcessedTokenCount(int processedTokenCount) {
		this.processedTokenCount = processedTokenCount;
	}


	public int getProcessedFileOrObjectCount() {
		return processedFileOrObjectCount;
	}


	public void setProcessedFileOrObjectCount(int processedFileOrObjectCount) {
		this.processedFileOrObjectCount = processedFileOrObjectCount;
	}


}

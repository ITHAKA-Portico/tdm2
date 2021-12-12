package org.portico.tdm.tdm2.tdmv2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.portico.tdm.tdm2.datawarehouse.Identifier;
import org.portico.tdm.tdm2.tools.TDMUtil;
import org.portico.tdm.tdm2.tools.ValidationUtils;
import org.portico.tdm.tdm2.tools.WebPageReader;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

/**
 * Use id: "https://chroniclingamerica.loc.gov/lccn/sn85038615/1903-01-27/ed-1"
 * @author dxie
 *
 */
public class V2NewspaperIssue extends TDMV2JsonObject{
	
	@JsonIgnore
	static Logger logger = LogManager.getLogger(V2NewspaperIssue.class.getName());
	@JsonIgnore
	static String programName = "V2NewspaperIssue";
	
	@JsonIgnore
	String collectionId;//Chronicling America
	
	@JsonIgnore
	String lccn;		//"sn 85038615" or "sn85038615"
	
	@JsonIgnore
	String issn;
	
	@JsonIgnore
	String oclc;		//"12872288"
	
	@JsonIgnore
	String subDir;
	
	@JsonIgnore
	String outputDir;
	
	@JsonIgnore
	String inputDir;
	
	@JsonIgnore
	boolean prettyFlag;
	
	@JsonIgnore
	V2Newspaper parentNewspaper;
	
	@JsonIgnore
	int edition;
	
	@JsonIgnore
	String batchName;
	
	@JsonIgnore
	List<String> pageUrls;
	

	
	/**
	 * 
	 * @param issueId "https://chroniclingamerica.loc.gov/lccn/sn85038615/1903-01-27/ed-1"
	 * @param issueId 
	 */
	public V2NewspaperIssue(String collectionId, String subdir, String issueId) {
		super();
		setArkId( issueId );
		setDocType("newspaper");
		setSubDir(subdir);
		setOutputDir("output" );
		setInputDir("input");
		
		Pattern p = Pattern.compile("https://chroniclingamerica.loc.gov/lccn/(.*)/(\\d{4}-\\d{2}-\\d{2})/ed-(\\d)");
		Matcher m = p.matcher(issueId);
		
		if (m.find()) {
			String lccn = m.group(1);
			String issue_date = m.group(2);
			String editionStr = m.group(3);
			
			setCollectionId("Chronicling America");
			setLccn(lccn);
			setDatePublished(issue_date);
			setEdition( Integer.parseInt(editionStr) );
			setUrl(issueId + "/");
			setProvider("loc-chronam");
			setFullTextAvailable(true);
			setOutputFormat(new ArrayList<String>(Arrays.asList("unigram", "fullText")) );
			
		}
		
		setCollectionId(collectionId);
		prettyFlag = false;
		
	}
	
	public V2NewspaperIssue(String subdir, String collectionId, String lccnNo, String issue_date, int edition) {
		super();
		setSubDir(subdir);
		setOutputDir("output");
		setInputDir("input");
		
		if ( collectionId.equalsIgnoreCase("Chronicling America")) {
			String lccn = lccnNo.replaceAll("\\s+", "").toLowerCase().trim();
			String newspaper_id = "https://chroniclingamerica.loc.gov/lccn/" + lccn ;
			String issueId = newspaper_id + "/" + issue_date + "/ed-" + edition;
			setArkId(issueId);
			setLccn(lccn);
			setDatePublished(issue_date);
			setUrl(issueId + "/");
			setProvider("loc-chronam");
			setFullTextAvailable(true);
			setOutputFormat(new ArrayList<String>(Arrays.asList("unigram", "fullText")) );
		}
		
		setCollectionId(collectionId);
		
		setDocType("newspaper");
		prettyFlag = false;
		
	}
	

	/**
	 * Parse an issue json file, ie https://chroniclingamerica.loc.gov/lccn/sn85038615/1903-02-01/ed-1.json
	 * @throws Exception 
	 */
	public void populateIssueMetadata() throws Exception {
		
		String id = getArkId();
		
		V2Newspaper parent = getParentNewspaper();
		String newspaper_title = parent.getTitle();
		setIsPartOf(newspaper_title);
		String issue_title = newspaper_title + ", issue " + getDatePublished();
		setTitle(issue_title);
		String pub_year_str = getDatePublished().substring(0, 4);
		int pub_year = Integer.parseInt(pub_year_str);
		setPublicationYear(pub_year);
		setPublisher( parent.getPublisher());
		String pubPlace = parent.getPlaceOfPublication();
		setPlaceOfPublication( pubPlace );
		setLanguage(parent.getLanguage());
		copyParentNewspaperIdentifiers();
		setSourceCategory(getParentNewspaper().getSourceCategory());
		
		//use json file to get other metadata
		JSONObject jo = null;
		try {
			jo = TDMUtil.getJsonObjectFromUrl( id + ".json");
		} catch (Exception e) {
			logger.error( programName + ":populateIssueMetadata :getJsonObjectFromUrl " + e );
			throw e;
		}
		
		//get batch name
		Map batch = ((Map)jo.get("batch")); 
		String batch_name =null;
		String batch_url = null;
		// iterating batch Map 
        Iterator<Map.Entry> itr1 = batch.entrySet().iterator(); 
        while (itr1.hasNext()) { 
            Map.Entry pair = itr1.next(); 
            if ( pair.getKey().equals("url")) {
            	batch_url = (String) pair.getValue();
            }
            if ( pair.getKey().equals("name")) {
            	batch_name = (String) pair.getValue();
            }
        } 
        if ( batch_name!=null ) {
        	setBatchName(batch_name);
        }
        
        //get pages
        JSONArray page_array = (JSONArray) jo.get("pages"); 
        Iterator itr2 = page_array.iterator(); 
        List<String> page_urls = new ArrayList<>();
        
        while (itr2.hasNext()){
            itr1 = ((Map) itr2.next()).entrySet().iterator(); 
            String url = null;
            int sequence = -1;
            while (itr1.hasNext()) { 
                Map.Entry pair = (Entry) itr1.next(); 
 
                if ( pair.getKey().equals("url")) {
                	url = (String) pair.getValue();				//https://chroniclingamerica.loc.gov/lccn/sn85038615/1903-02-01/ed-1/seq-1.json
                }
                /*if ( pair.getKey().equals("sequence")) {
                	sequence = (int) pair.getValue();
                }*/
            }
            
            if ( url != null ) {
             	page_urls.add(url);
            }

        } 
        
        setPageUrls(page_urls);
        setPageCount( page_urls.size());
        setPageStart("1");
        setPageEnd(page_urls.size() + "");
        
	}

	private void copyParentNewspaperIdentifiers() {
		V2Newspaper newspaper = getParentNewspaper();
		List<Identifier> ids = new ArrayList<>();
		String lccn = getLccn();
		Identifier lccn_id = null;
		try {
			lccn_id = new Identifier("lccn", lccn);
			ids.add(lccn_id);
			
		} catch (Exception e) {
			logger.error( programName + ":copyParentNewspaperIdentifiers Error creating lccn identifier " + lccn + " " + e);
		}
		
		String issn = newspaper.getIssn();
		setIssn(issn);
		try {
			Identifier issn_id = new Identifier("issn", issn);

			ids.add(issn_id);
		} catch (Exception e) {
			logger.info( programName + ":copyParentNewspaperIdentifiers Error creating issn identifier " + lccn + " " + e);
		}
		
		String oclc = newspaper.getOclc();
		setOclc(oclc);
		try {
			Identifier oclc_id = new Identifier("oclc", oclc);

			ids.add(oclc_id);
		} catch (Exception e) {
			logger.info( programName + ":copyParentNewspaperIdentifiers Error creating oclc identifier " + lccn + " " + e);
		}
		
		setIdentifiers(ids);
	}

	/**
	 * 
	 * @param local If true, get fulltext from local drive. If false, read from Url.
	 */
	public void performNLPTask(boolean local) {
		
		String id = getArkId();
		
		List<String> page_urls = getPageUrls();
		List<String> pageFullTextList = new ArrayList<>();
		WebPageReader reader = new WebPageReader();
		
		for(String page_json_url: page_urls ) {
			//use page json to get ocr text address
			if ( local ) {
				String page_text;
				try {
					page_text = readLocalPageText( page_json_url );
					page_text = page_text.replaceAll("\\r\\n", "\\n");
				} catch (Exception e) {
					logger.error( programName + ":performNLPTask :readLocalPageText Error reading page ocr file " + page_json_url + " " + e );
					continue;
				}
				pageFullTextList.add(page_text);
			}
			else {
				JSONObject jo = null;
				try {
					jo = TDMUtil.getJsonObjectFromUrl( page_json_url);
				} catch (Exception e) {
					logger.error( programName + ":performNLPTask :getJsonObjectFromUrl Error reading page json file " + page_json_url + " " + e );
					continue;
				}

				//get text
				String text_url = (String) jo.get("text");			//https://chroniclingamerica.loc.gov/lccn/sn85038615/1903-02-01/ed-1/seq-1/ocr.txt
				try {
					String page_text = reader.getTextFromUrl(text_url);
					page_text = page_text.replaceAll("\\r\\n", "\\n");
					pageFullTextList.add(page_text);

				} catch (IOException e) {
					logger.error(programName + ":performNLPTask :getTextFromUrl Error getting text from page " + text_url + " " + e );
				}
			}
		
		}


		//join page text into 1 string
		String fulltext = String.join("\n", pageFullTextList); 
		setFullText(pageFullTextList);
		
		Properties props = new Properties();
        //props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        props.setProperty("annotators", "tokenize");		//simple version

        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(fulltext);

        // run all Annotators on this text
        pipeline.annotate(document);

        //
        List<CoreLabel> tokens = document.get(TokensAnnotation.class);
        List<String> words = tokens.stream().map(token->token.get(TextAnnotation.class)).collect(Collectors.toList());
        
        int token_count = tokens.size();
        setWordCount(token_count);
        
      //Ted requsted to not include
       // Map<String, Integer> wordCountMap = TDMUtil.generateUnigramMapFromWords( words );
        //setUnigramCount(wordCountMap);
		
	}
	
	/**
	 * Read text from input/chronam/sn84024827/1908/03/24/ed-1/seq-1/ocr.txt
	 * @param page_json_url  https://chroniclingamerica.loc.gov/lccn/sn91037345/1893-01-04/ed-1/seq-1.json
	 * @return
	 * @throws Exception 
	 */
	private String readLocalPageText(String page_json_url) throws Exception {
		String pagetext = null;
		String lccn = getLccn();
		String issue = getDatePublished();
		
		//https://chroniclingamerica.loc.gov/lccn/sn86091254/1959-11-01/ed-1/seq-1.json
		Pattern p = Pattern.compile("https://chroniclingamerica.loc.gov/lccn/" + lccn + "/" + issue + "/ed-\\d*/seq-(\\d*).json");
		Matcher m = p.matcher(page_json_url);
		String page_seq = null;
		if ( m.find()) {
			page_seq = m.group(1);
		}
		else {
			logger.error( programName + ":readLocalPageText :Error getting page seq# from " + page_json_url);
			throw new Exception("Error getting page seq #");
		}
		

		String filepath = inputDir + File.separator + getSubDir() + File.separator + getLccn() + File.separator + getDatePublished().replace("-", File.separator) 
						+ File.separator + "ed-" + getEdition() + File.separator + "seq-" + page_seq + File.separator +  "ocr.txt" ;
		
		
		try {
			pagetext = new String(Files.readAllBytes(Paths.get(filepath)));
		} catch (IOException e) {
			logger.error( programName + ":readLocalPageText :Error read text from " + filepath );
			throw new Exception("Error read text ");
		}
		
		return pagetext;
	}

	/**
	 * 
	 * @return The created json file name (with path) ie output/chronam/sn82015411/sn82015411_1919-11-25_ed-1.json
	 * @throws IOException
	 * @throws ProcessingException 
	 */
	public String outputJson() throws IOException, ProcessingException {
		
		String outputDir = getOutputDir();

		//create output directory if it doesn't exist
		Path dirPathObj = Paths.get( outputDir + File.separator + getSubDir() + File.separator + getLccn()  );

		if( ! Files.exists(dirPathObj)) {

			try {
				// Creating The New Directory Structure
				Files.createDirectories(dirPathObj);

			} catch (IOException ioExceptionObj) {
				logger.error( programName + ":outputJson :createDirectories: Error creating directory " + dirPathObj.getFileName() + " " + ioExceptionObj.getMessage());
				throw ioExceptionObj;

			}
		}

		String jsonStr = toJsonStr( prettyFlag );
		String issueid = getArkId();												//"https://chroniclingamerica.loc.gov/lccn/sn85038615/1903-01-27/ed-1"
		String filename = getLccn() + "_" + getDatePublished() + "_ed-" + getEdition() + ".json";		//sn85038615_1903-01-27_ed-1.json
		List<String> lines = Arrays.asList(jsonStr);

		Path file = Paths.get( outputDir + File.separator + getSubDir() + File.separator + getLccn() + File.separator + filename );

		try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			logger.error( programName + ":outputJson: Error writing to " + file.toString() + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		//validate against schema
		File schemaFile = new File("output" + File.separator + "metadata.schema.json");
        File jsonFile = new File("output" + File.separator + "chronam" + File.separator + getLccn() + File.separator + filename );
        	
        try {
			if ( ! ValidationUtils.isJsonValid(schemaFile, jsonFile)){
				logger.error( programName + ":outputJson: Json file not validate against schema file "+ jsonFile.getName());
			}
			
		} catch (ProcessingException e) {
			logger.error( programName + ":outputJson: Error validating output json file " + file.toString() + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		return file.toString();

	}


	private String toJsonStr(boolean pretty_flag ) {
		String jsonStr = null;
		
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);  	//exclude null fields
		mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        try
        {
            
            if ( pretty_flag ) {
            	jsonStr = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
            }
            else {
            	jsonStr = mapper.writeValueAsString(this);
            }
 
            /*List<String> lines = Arrays.asList(jsonStr);
            Path file = Paths.get( getId().substring( getId().lastIndexOf("/") + 1 ) + ".json");
            Files.write(file, lines, Charset.forName("UTF-8"));*/
 
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
		
        //System.out.println(jsonStr);
        
		return jsonStr;
	}
	

	public void logToDB() {
		
		String lccn = getLccn();
		String issue_date = getDatePublished();
		int edition = getEdition();
		String title = getTitle();
		String publisher = getPublisher();
		int pub_year = getPublicationYear();
		String pub_place = getPlaceOfPublication();
		String batch_name = getBatchName();
		int pageCount = getPageCount();
		String url = getUrl();
		String issn = getIssn();
		String oclc = getOclc();
		String collectionId = getCollectionId();
		
		String existance_query = "select count(*) from tdm_newspaper where lccn='" + lccn + "' and issue='" + issue_date + 
				"' and type='newspaper' and edition=" + edition;
		
		String insert_query = "insert into TDM_NEWSPAPER ( title, publisher, lccn, issn, oclc, issue, url, batch_name, type, pub_year, pub_date, "
							+ " pub_place, edition, page_count, creation_ts, collection_id) values (?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?, ?)";
		
		boolean newInsert = true;
		

		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement existance_stmt = conn.createStatement();
				PreparedStatement insert_stmt = conn.prepareStatement(insert_query);
				Statement update_stmt = conn.createStatement(); ) {
			
			ResultSet rs = existance_stmt.executeQuery(existance_query);
		
			if (rs.next()) {
				int count = rs.getInt(1);
				
				if ( count == 1 ) {
					newInsert = false;
				}
			}
			
			rs.close();
			
			if ( newInsert ) {	//this issue has not been logged before

				insert_stmt.setString(1, title);
				insert_stmt.setString(2, publisher);
				insert_stmt.setString(3, lccn);
				insert_stmt.setString(4, issn);
				insert_stmt.setString(5, oclc);
				insert_stmt.setString(6, issue_date);
				insert_stmt.setString(7, url);
				insert_stmt.setString(8,  batch_name);
				insert_stmt.setString(9,  "newspaper");
				insert_stmt.setInt(10, pub_year);
				insert_stmt.setString(11,  issue_date);
				insert_stmt.setString(12,  pub_place);
				insert_stmt.setInt(13, edition);
				insert_stmt.setInt(14, pageCount);
				Timestamp sqlDate = new java.sql.Timestamp( new java.util.Date().getTime());
				insert_stmt.setTimestamp(15, sqlDate);
				insert_stmt.setString(16,  collectionId);
				
				insert_stmt.executeUpdate();

			}
			else {	//this au has been inserted before
				
				String update_query = "update tdm_newspaper set title =q'$" + title + "$', publisher=q'$" + publisher + "$', type='newspaper', collection_id='" + collectionId + "', "
				          + " issn='" + issn + "', oclc='" + oclc + "', url=q'$" + url + "$', batch_name='" + batch_name + "', pub_year=" + pub_year 
				          + ", pub_place=q'$" + pub_place + "$', edition=" + edition + ", page_count=" + pageCount + ", modification_ts=current_timestamp "
				          + " where lccn='" + lccn + "' and issue='" + issue_date + "'";

				update_stmt.executeQuery(update_query);
			}
			
		
		}
		catch(  Exception  e) {
				
			logger.error( programName + ":logToDB:  " + e);
			e.printStackTrace();
		}
		
	}
	
	
	
	public String getCollectionId() {
		return collectionId;
	}

	public void setCollectionId(String collectionId) {
		this.collectionId = collectionId;
	}

	public String getLccn() {
		return lccn;
	}

	public void setLccn(String lccn) {
		this.lccn = lccn;
	}

	public String getIssn() {
		return issn;
	}

	public void setIssn(String issn) {
		this.issn = issn;
	}

	public String getOclc() {
		return oclc;
	}

	public void setOclc(String oclc) {
		this.oclc = oclc;
	}

	public String getSubDir() {
		return subDir;
	}

	public void setSubDir(String subDir) {
		this.subDir = subDir;
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

	public boolean isPrettyFlag() {
		return prettyFlag;
	}

	public void setPrettyFlag(boolean prettyFlag) {
		this.prettyFlag = prettyFlag;
	}

	public V2Newspaper getParentNewspaper() {
		return parentNewspaper;
	}

	public void setParentNewspaper(V2Newspaper parentNewspaper) {
		this.parentNewspaper = parentNewspaper;
	}

	public int getEdition() {
		return edition;
	}

	public void setEdition(int edition) {
		this.edition = edition;
	}

	public String getBatchName() {
		return batchName;
	}

	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}

	public List<String> getPageUrls() {
		return pageUrls;
	}

	public void setPageUrls(List<String> pageUrls) {
		this.pageUrls = pageUrls;
	}


}

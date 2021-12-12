package org.portico.tdm.tdm2.tdmv2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.portico.tdm.tdm2.tools.TDMUtil;
import org.portico.tdm.tdm2.tools.WebPageReader;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class V2Newspaper extends TDMV2JsonObject{
	
	@JsonIgnore
	static Logger logger = LogManager.getLogger(V2Newspaper.class.getName());
	@JsonIgnore
	static String programName = "V2Newspaper";
	
	@JsonIgnore
	String collectionId;		//Chronicling America
	
	@JsonIgnore
	String lccn;		//"sn 85038615"
	
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
	List<V2NewspaperIssue> issues;
	
	@JsonIgnore
	List<String> issueJsonCreated;
	
	@JsonIgnore
	List<String> issueJsonFailed;
	
	@JsonIgnore
	List<String> issueJsonFilesMissing;
	
	@JsonIgnore
	List<String> jsonLineFilesUploaded;

	@JsonIgnore
	List<String> jsonLineFilesFailed;

	public V2Newspaper(String collectionId, String lccnNo, String subdir) {
		super();
		
		setSubDir(subdir);
		
		if ( collectionId.equalsIgnoreCase("Chronicling America")) {
			String lccn = lccnNo.replaceAll("\\s+", "").toLowerCase().trim();
			String id = "https://chroniclingamerica.loc.gov/lccn/" + lccn ;
		
			setArkId(id);
			setLccn(lccn);
			setUrl(id + "/");
			setProvider("loc-chronam");
			setOutputDir("output" );
			setInputDir("input");
			setFullTextAvailable(true);
			
		}
		
		setCollectionId(collectionId);
		setDocType("newspaper");
		prettyFlag = false;
		
		issueJsonCreated = new ArrayList<>();
		issueJsonFailed = new ArrayList<>();
		issueJsonFilesMissing = new ArrayList<>();
		jsonLineFilesUploaded = new ArrayList<>();
		jsonLineFilesFailed = new ArrayList<>();

	}
	
	
	/**
	 * Parse https://chroniclingamerica.loc.gov/lccn/sn85038615.json
	 * @throws Exception 
	 */

	public void populateMetadata() throws Exception {
		
		String id = getArkId();	//https://chroniclingamerica.loc.gov/lccn/sn85038615
		
		//use rdf file to get issn# and oclc#
		String rdf_url = id + ".rdf";
		WebPageReader webreader = new WebPageReader();
		webreader.readUrl(rdf_url);
		List<String> lines = webreader.retrieveLineWithPattern("owl:sameAs");
		Pattern p = Pattern.compile("<owl:sameAs rdf:resource=\"(.*)\"/>");		//<owl:sameAs rdf:resource="urn:issn:1941-0700"/>
		for(String line: lines) {
			Matcher m = p.matcher(line);
			if ( m.find()) {
				String resource = m.group(1);
				
				if ( resource.startsWith("urn:issn")) {				////<owl:sameAs rdf:resource="urn:issn:1941-0700"/>
					String issn = resource.replace("urn:issn:", "");
					setIssn(issn);
				}
				else if ( resource.startsWith("info:oclcnum")) {				//<owl:sameAs rdf:resource="info:oclcnum/12872288"/>
					String oclc = resource.replace("info:oclcnum/", "");
					setOclc(oclc);
				}
			}
		}
		//use rdf to get language		<dcterms:language rdf:resource="http://www.lingvoj.org/lang/en"/>
		List<String> lans = new ArrayList<>();
		lines = webreader.retrieveLineWithPattern("dcterms:language");
		Pattern p2 = Pattern.compile("<dcterms:language rdf:resource=\"(.*)\"/>");		
		
		for(String line: lines) {
			Matcher m = p2.matcher(line);
			if ( m.find()) {
				String resource = m.group(1);
				
				String lan2 = resource.substring(resource.lastIndexOf("/") + 1 );
				String lang3letter = TDMUtil.convertLan2ToLan3( lan2 );
				if ( lang3letter != null ) {
					lans.add(lang3letter);
				}
			}
		}
		setLanguage(lans);
		
		//use json file to get other metadata
		JSONObject jo = null;
		try {
			jo = TDMUtil.getJsonObjectFromUrl( id + ".json");
		} catch (Exception e) {
			logger.error( programName + ":populateMetadata :getJsonObjectFromUrl " + e );
			throw e;
		}
        
        //get title
        String name = (String) jo.get("name");
          
        // getting publisher info
        String placeOfPublication = (String) jo.get("place_of_publication"); 
        String publisher = (String) jo.get("publisher");
        setPublisher(publisher);
        setPlaceOfPublication(placeOfPublication);
        
        //get years
        String startYear = (String) jo.get("start_year"); 
        String endYear = (String) jo.get("end_year"); 
          
        String title = name + " : (" + placeOfPublication + ") " + startYear + "-" + endYear;
        setTitle( title );
        
        // getting subjects 
        JSONArray subject = ((JSONArray)jo.get("subject")); 
        Iterator itr1 = subject.iterator(); 
        List<String> subjects = new ArrayList<>();
        
        while (itr1.hasNext())  
        { 
            String subj = (String) itr1.next();
            subjects.add(subj);
        }  
        setSourceCategory(subjects);
        
       
	}
	

	/**
	 * Create List<V2NewspaperIssue> objects by reading newspaper json file "issues" array
	 * @param checkflag If set to true and an issue has already been created in tdm_newspaper table, then do not add this issue to the result issue list.
	 * @throws Exception
	 */
	public void createIssues(boolean checkflag) throws Exception  {
		
		String id = getArkId();	//https://chroniclingamerica.loc.gov/lccn/sn85038615
		
		JSONObject jo = null;
		try {
			jo = TDMUtil.getJsonObjectFromUrl( id + ".json");
		} catch (Exception e) {
			logger.error( programName + ":createIssues :getJsonObjectFromUrl " + e );
			throw e;
		}
        
        // getting issues
        JSONArray issues_array = (JSONArray) jo.get("issues"); 
        Iterator itr2 = issues_array.iterator(); 
        List<V2NewspaperIssue> issues = new ArrayList<>();
        
        String lccn = getLccn();
        
        
        String query = "select count(*)  from tdm_newspaper where lccn='" + lccn + "' and issue=? and url=?";
        
        try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
        		PreparedStatement stmt = conn.prepareStatement(query) ) {

        	int index = 1;
        	while (itr2.hasNext()){
        		Iterator itr1 = ((Map) itr2.next()).entrySet().iterator(); 
        		String url = null;
        		String date_issued = null;
        		while (itr1.hasNext()) { 
        			Map.Entry pair = (Entry) itr1.next(); 

        			if ( pair.getKey().equals("url")) {				//url = https://chroniclingamerica.loc.gov/lccn/sn86091254/1960-01-01/ed-1.json
        				url = (String) pair.getValue();
        			}
        			if ( pair.getKey().equals("date_issued")) {
        				date_issued = (String) pair.getValue();
        			}
        		}

        		if ( url != null ) {
        			String issueId = url.replace(".json", "") ;			//https://chroniclingamerica.loc.gov/lccn/sn86091254/1960-01-01/ed-1  --> use as issue arkid


        			if ( checkflag ) {
        				
        				stmt.setString(1,  date_issued);
        				stmt.setString(2,  issueId+ "/");       		//url in tdm_newspaper table: https://chroniclingamerica.loc.gov/lccn/sn86091254/1960-01-01/ed-1/
        				ResultSet resultSet = stmt.executeQuery();
        				
        				if (resultSet.next()) {
        					int count = resultSet.getInt(1);
        					
        					if ( count == 1 ) {
        						logger.info(programName + ":createIssues : skip existing issue " + issueId );
        						continue;		//do not create this issue object
        					}
        				}
        				
        				resultSet.close();
         			}

        			V2NewspaperIssue issue = new V2NewspaperIssue(getCollectionId(), getSubDir(), issueId);
        			issue.setParentNewspaper(this);
        			issue.setSequence(index++);
        			issues.add(issue);
        		}

        	} 
        }
        catch(Exception e) {
        	logger.error(programName + ":createIssues : " + e );
        	throw e;
        }
        
        setIssues(issues);
	}


	public List<String> processIssues(boolean local, boolean checknew) throws Exception  {
		
		List<V2NewspaperIssue> issues = getIssues();
		
		if (issues == null || issues.isEmpty()) {
			if ( checknew ) {		//possible there is no new issue json files been created
				logger.info( programName + ":processIssues: Empty issues to process " + getLccn() );
				return null;
			}
			
			logger.error( programName + ":processIssues: Empty issue in title " + getLccn() );
			throw new Exception("Empty issue in title");
		}
		
		List<String> jsonFileList = new ArrayList<>();
		
		for(V2NewspaperIssue issue: issues) {
			String issue_id = issue.getArkId();

			
			try {
				issue.populateIssueMetadata();
			} catch (Exception e1) {
				logger.error( programName + ":processIssues: populateIssueMetadata: " + issue_id + " " + e1);
				issueJsonFailed.add(issue_id);
				e1.printStackTrace();
				continue;
			}
			
			issue.performNLPTask(local);
			
			//log some AU info in TDM_AU table
			try {
				issue.logToDB();
			} catch (Exception e) {
				logger.error( programName + ":processIssues: logToDB: " + issue_id + " " + e);
				e.printStackTrace();
				
			}

			try {
				String jsonfile = issue.outputJson();
				jsonFileList.add(jsonfile);
				
			} catch (IOException e) {
				logger.error( programName + ":processIssues :outputJson: " + issue_id + " " + e);
				e.printStackTrace();
				issueJsonFailed.add(issue_id);
				continue;
			}
			
			issueJsonCreated.add(issue_id);

		}
		
		return jsonFileList;
		
	}
	


	/**
	 * 
	 * @param local If true, read issue ocr text file from local drive. If false, read ocr txt from url address
	 * @param issue_date
	 * @return json file with relative path, ie output/chronam/sn82015411/sn82015411_1919-11-25_ed-1.json
	 * @throws Exception
	 */
	public String processOneIssue(boolean local, String issue_date) throws Exception {
		
		String issue_id = getArkId() + "/" + issue_date + "/ed-1";
		String jsonFile = null;
		
		V2NewspaperIssue issue = new V2NewspaperIssue(getCollectionId(), getSubDir(), issue_id);
    	issue.setParentNewspaper(this);

    	try {
			issue.populateIssueMetadata();
		} catch (Exception e1) {
			logger.error( programName + ":processIssues: populateIssueMetadata: " + issue_id + " " + e1);
			issueJsonFailed.add(issue_id);
			e1.printStackTrace();
			throw e1;
		}
		
		issue.performNLPTask( local );
		
		//log some AU info in TDM_AU table
		try {
			issue.logToDB();
		} catch (Exception e) {
			logger.error( programName + ":processIssues: logToDB: " + issue_id + " " + e);
			e.printStackTrace();
			throw e;
		}

		try {
			jsonFile = issue.outputJson();
			
		} catch (IOException e) {
			logger.error( programName + ":processIssues :outputJson: " + issue_id + " " + e);
			issueJsonFailed.add(issue_id);
			e.printStackTrace();
			throw e;
		}
		
		issueJsonCreated.add(jsonFile);
		
		return jsonFile;
	}

	/**
	 * Upload issues searched from tdm_newspaper table.
	 * @param combineBySize
	 * @return 
	 * @throws Exception 
	 */
	public List<String> uploadTableIssues(int combinesize) throws Exception {
		
		List<String> uploadedJsonLineFiles = new ArrayList<>();
		
		//Get json file list from tdm_newspaper table
		List<String> jsonFileList = null;
		try {
			jsonFileList = findIssuesFromTable();
		} catch (Exception e) {
			logger.info( programName + ":uploadIssues: findIssuesFromTable: Error finding issue json file list " + getLccn() );
			throw new Exception("Error finding issue json file list");
		}
		
		uploadedJsonLineFiles = uploadIssues(combinesize, jsonFileList ); 
		
		return uploadedJsonLineFiles;
	}



	

	private List<String> findIssuesFromTable() throws Exception {
		List<String> jsonfiles = new ArrayList<>();
		String lccn = getLccn();
		
		String query = "select * from tdm_newspaper where lccn='" + lccn + "' order by pub_date";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement existance_stmt = conn.createStatement(); ) {
			
			ResultSet rs = existance_stmt.executeQuery(query);
		
			while (rs.next()) {
				String pub_date = rs.getString("pub_date");
				String edition = rs.getString("edition");
				//construct json file  output/chronam/sn82015133/sn82015133_1880-12-25_ed-1.json
				String jsonfile = inputDir + File.separator + getSubDir() + File.separator + lccn + File.separator + lccn+ "_" + pub_date + "_" + "ed-" + edition + ".json"  ;
				jsonfiles.add(jsonfile);
			}
			
			rs.close();
		}
		catch(Exception e) {
			logger.info( programName + ":findIssuesFromTable: Error finding issue json file list " + getLccn() );
			throw new Exception("Error finding issue json file list");
		}
		
		
		return jsonfiles;
	}


	/*
	 * 
	 */
	public List<String> uploadIssues(int combinesize, List<String> jsonFileList ) throws Exception {
		
		List<String> uploadedJsonLineFiles = new ArrayList<>();
		
		String lccn = getLccn();
		
		if (jsonFileList == null || jsonFileList.isEmpty()) {
			logger.info( programName + ":uploadIssues: Empty json file list " + getLccn() );
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

			String jsonlinefilename = lccn + "_" + tsStr + "_" + i + ".jsonl";
			String jsonlinefileWithPath = outputDir + File.separator + subDir + File.separator + jsonlinefilename; 
			
			try {
				List<String> missingJsonFiles = TDMUtil.concatenateJsonFilesToOneFile( sliceList, jsonlinefileWithPath );
				issueJsonFilesMissing.addAll(missingJsonFiles);
	
				jsonLineFilesUploaded.add(jsonlinefileWithPath);
				uploadedJsonLineFiles.add(jsonlinefileWithPath);
				
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
		List<String> gzipJsonLineFile = TDMUtil.gzipFiles( jsonLineFilesUploaded );  

		//upload gzipped jsonline files
		TDMUtil.uploadFiles( gzipJsonLineFile );
		

		//log upload timestamp to TDM_NEWSPAPER table
		try   {
			TDMUtil.logNewspaperIssueUploadTimeToDB( jsonFileList );
		}
		catch(Exception e) {
			logger.error( programName + ":uploadIssues " + e.getMessage());
		}
		
		return uploadedJsonLineFiles;	
	}



	public String uploadOneIssue(String jsonFile) throws IOException {
		
		String jsonlinefilename = jsonFile.substring(jsonFile.lastIndexOf(File.separator) + 1 ).replace(".json", ".jsonl");
		
		String jsonlinefileWithPath = outputDir + File.separator + subDir + File.separator + jsonlinefilename;
		
		//copy json file to json line file
		try {
			Files.copy(Paths.get(jsonFile), new FileOutputStream(jsonlinefileWithPath));
		} catch (FileNotFoundException e1) {
			logger.error( programName + ": uploadOneIssue " + jsonFile + " " + e1);
			jsonLineFilesFailed.add(jsonlinefileWithPath);
			throw e1;	
		} catch (IOException e1) {
			logger.error( programName + ": uploadOneIssue " + jsonFile + " " + e1);
			jsonLineFilesFailed.add(jsonlinefileWithPath);
			throw e1;		
		}
		
		String gzipJsonLineFile = TDMUtil.gzipFile( jsonlinefileWithPath );  
		
		//upload gzipped jsonline files
		TDMUtil.UploadGzipJsonLineFile( gzipJsonLineFile );
		

		//log upload timestamp to TDM_NEWSPAPER table
		try   {
			TDMUtil.logNewspaperIssueUploadTimeToDB( new ArrayList<String>(Arrays.asList(jsonFile))) ;
		}
		catch(Exception e) {
			logger.error( programName + ":uploadIssues " + e.getMessage());
		}
		
		jsonLineFilesUploaded.add(jsonlinefileWithPath);
		
		return jsonlinefileWithPath;
		
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


	public List<V2NewspaperIssue> getIssues() {
		return issues;
	}


	public void setIssues(List<V2NewspaperIssue> issues) {
		this.issues = issues;
	}


	public List<String> getIssueJsonCreated() {
		return issueJsonCreated;
	}


	public void setIssueJsonCreated(List<String> issueJsonCreated) {
		this.issueJsonCreated = issueJsonCreated;
	}


	public List<String> getIssueJsonFailed() {
		return issueJsonFailed;
	}


	public void setIssueJsonFailed(List<String> issueJsonFailed) {
		this.issueJsonFailed = issueJsonFailed;
	}

	public List<String> getIssueJsonFilesMissing() {
		return issueJsonFilesMissing;
	}


	public void setIssueJsonFilesMissing(List<String> issueJsonFilesMissing) {
		this.issueJsonFilesMissing = issueJsonFilesMissing;
	}


	public List<String> getJsonLineFilesUploaded() {
		return jsonLineFilesUploaded;
	}


	public void setJsonLineFilesUploaded(List<String> jsonLineFilesUploaded) {
		this.jsonLineFilesUploaded = jsonLineFilesUploaded;
	}


	public List<String> getJsonLineFilesFailed() {
		return jsonLineFilesFailed;
	}


	public void setJsonLineFilesFailed(List<String> jsonLineFilesFailed) {
		this.jsonLineFilesFailed = jsonLineFilesFailed;
	}


}

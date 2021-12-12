package org.portico.tdm.tdm2.tdmv2;

import java.io.File;
import java.io.IOException;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.tools.TDMUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class V2Chapter  extends TDMV2JsonObject {
	
	@JsonIgnore
	static Logger logger = LogManager.getLogger(V2Chapter.class.getName());
	@JsonIgnore
	static String programName = "V2Chapter";
	
	@JsonIgnore
	String subDir;
	
	@JsonIgnore
	String publisherId;    	//this is the provider's publisherID, cmi_providers.provider_id, ie MUSE
	
	@JsonIgnore
	String outputDir;
	
	@JsonIgnore
	String inputDir;
	
	@JsonIgnore
	boolean prettyFlag;
	
	@JsonIgnore
	String bitsXmlFileName;
	
	@JsonIgnore
	String pdfFile;		//TODO:  1 pdf for each chapter?
	
	@JsonIgnore
	String xmlFile;

	@JsonIgnore
	String fulltextFrom;			//"pdf", "xml", "html", "xml header", "no fulltext", "AU does not exist"
	
	@JsonIgnore
	V2Book parentBook;
	
	@JsonIgnore
	String bookAuid;
	
	public V2Chapter(String suid) {
		
		super();
		
		setAuid(suid);
		setArkId( suid.replace("ark:/", "ark://") );
		setDocType("chapter");
		setProvider("portico");
		
		prettyFlag = false;
		
	}
	

	public void performChapterNLPTask() throws Exception {
		
		String auid = getBookAuid();
		String chapterpdf = getPdfFile();
		
		String pdfFileWithPath = inputDir + File.separator + subDir + File.separator +
								auid.substring(auid.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + chapterpdf;

		List<String> chapterFullTextByPage = TDMUtil.getFullTextFromPDF( pdfFileWithPath );
		setFullText( chapterFullTextByPage);
		if ( chapterFullTextByPage != null && ! chapterFullTextByPage.isEmpty()) {
			int pageCount = chapterFullTextByPage.size();
			setPageCount(pageCount);
			setFulltextFrom("pdf");
		}
		else {
			setFulltextFrom("no fulltext");
		}

		//join page text into 1 string
		String fulltext = String.join("\n", chapterFullTextByPage); 
		
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
        
        ////Ted requsted to not include
        //Map<String, Integer> wordCountMap = TDMUtil.generateUnigramMapFromWords( words );
        //setUnigramCount(wordCountMap);
	}
	

	public void logToDB() {
		
		String csname = getContentSetName();
		String au_id = getParentBook().getAuid();
		String su_id = getAuid();
		String publisherid = getPublisherId();
		int pub_year = getPublicationYear();
		
		String pdfFile = getPdfFile() ;
		String xmlFile = getXmlFile() ;
		String oa = "";
		int seq = getSequence();
		
		if ( isFullTextAvailable() ) {
			oa = "Y";
		}
		else {
			oa = "N";
		}
		
		String existance_query = "select count(*) from tdm_book where au_id='" + au_id + "' and su_id='" + su_id + "' and type='chapter'";
		
		String insert_query = "insert into TDM_BOOK ( au_id, su_id, publisher_id, type, content_set_name, "
				+ " pdf_file, xml_file, oa, pub_year, creation_ts, seq ) values (?,?,?,?, ?,?,?,?,?,?, ?)";
		
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
			
			if ( newInsert ) {	//this au has not been logged before

				insert_stmt.setString(1, au_id);
				insert_stmt.setString(2, su_id);
				insert_stmt.setString(3, publisherid);
				insert_stmt.setString(4, "chapter");
				insert_stmt.setString(5, csname);
				insert_stmt.setString(6, pdfFile);
				insert_stmt.setString(7, xmlFile);
				insert_stmt.setString(8,  oa);
				insert_stmt.setInt(9, pub_year);
				Timestamp sqlDate = new java.sql.Timestamp( new java.util.Date().getTime());
				insert_stmt.setTimestamp(10, sqlDate);
				insert_stmt.setInt(11,  seq);

				insert_stmt.executeUpdate();

			}
			else {	//this au has been inserted before
				
				String update_query = "update tdm_book set publisher_id='" + publisherid + "', type='chapter', content_set_name=q'$" + csname + "$', seq=" + seq + ", ";
				if ( pdfFile != null ) {
					update_query += "pdf_file='" + pdfFile + "', ";
				}
				if ( xmlFile != null ) {
					update_query += "xml_file='" + xmlFile + "', ";
				}

				update_query += "oa='" + oa + "', pub_year=" + pub_year + ", modification_ts=current_timestamp where au_id='" + au_id + "' and su_id='" + su_id + "' and type='chapter'";

				update_stmt.executeQuery(update_query);
			}
		}
		catch(  Exception  e) {
				
			logger.error( programName + ":logToDB:  " + e);
			e.printStackTrace();
		}
	}



	public void outputJson() throws Exception {
		String outputDir = getOutputDir();

		String jsonStr = toJsonStr( prettyFlag );
		String suid = getAuid();
		String filename = suid.substring( suid.lastIndexOf("/") + 1) + ".json";		//ark:/27927/pgj2j7b70gv --> pgj2j7b70gv.json
        List<String> lines = Arrays.asList(jsonStr);
        
        Path dirPathObj = Paths.get( outputDir + File.separator + getSubDir()  );

        if( ! Files.exists(dirPathObj)) {
            
            try {
                // Creating The New Directory Structure
                Files.createDirectories(dirPathObj);
                
            } catch (IOException ioExceptionObj) {
            	logger.error( programName + ":outputJson:createDirectories: Error creating directory " + dirPathObj.getFileName() + " " + ioExceptionObj.getMessage());
                throw ioExceptionObj;

            }
        }
        
        Path file = Paths.get( outputDir + File.separator + getSubDir() + File.separator + filename );
        
        try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			logger.error( programName + ":outputJson: Error writing to " + file.toString() + " " + e.getMessage());
			e.printStackTrace();
			throw new Exception("Error writing to chapter json file " + filename );
		}
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


	public String getSubDir() {
		return subDir;
	}

	public void setSubDir(String subDir) {
		this.subDir = subDir;
	}

	public String getPublisherId() {
		return publisherId;
	}

	public void setPublisherId(String publisherId) {
		this.publisherId = publisherId;
	}

	public boolean isPrettyFlag() {
		return prettyFlag;
	}

	public void setPrettyFlag(boolean prettyFlag) {
		this.prettyFlag = prettyFlag;
	}

	public String getBitsXmlFileName() {
		return bitsXmlFileName;
	}

	public void setBitsXmlFileName(String bitsXmlFileName) {
		this.bitsXmlFileName = bitsXmlFileName;
	}

	public String getXmlFile() {
		return xmlFile;
	}

	public void setXmlFile(String xmlFile) {
		this.xmlFile = xmlFile;
	}

	public String getFulltextFrom() {
		return fulltextFrom;
	}

	public void setFulltextFrom(String fulltextFrom) {
		this.fulltextFrom = fulltextFrom;
	}

	public V2Book getParentBook() {
		return parentBook;
	}

	public void setParentBook(V2Book parentBook) {
		this.parentBook = parentBook;
	}


	public String getPdfFile() {
		return pdfFile;
	}


	public void setPdfFile(String pdfFile) {
		this.pdfFile = pdfFile;
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


	public String getBookAuid() {
		return bookAuid;
	}


	public void setBookAuid(String bookAuid) {
		this.bookAuid = bookAuid;
	}



	
}

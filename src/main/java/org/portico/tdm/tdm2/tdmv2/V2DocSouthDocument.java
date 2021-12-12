package org.portico.tdm.tdm2.tdmv2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.portico.tdm.tdm2.datawarehouse.Identifier;
import org.portico.tdm.tdm2.tools.XslTransform;
import org.portico.tdm.tdm2.tools.TDMUtil;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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

public class V2DocSouthDocument extends TDMV2JsonObject {
	@JsonIgnore
	static Logger logger = LogManager.getLogger(V2DocSouthDocument.class.getName());
	@JsonIgnore
	static String programName = "V2DocSouthDocument";
	@JsonIgnore
	static String XSLFILENAME = "text-with-pagebreak.xsl";
	
	@JsonIgnore
	String collectionId;		//DocSouth
	
	@JsonIgnore
	String docsouthCollId;		//fpn, church, southlit, neh
	
	@JsonIgnore
	String titleId;		// 
	
	@JsonIgnore
	String subDir;
	
	@JsonIgnore
	String outputDir;
	
	@JsonIgnore
	String inputDir;
	
	@JsonIgnore
	boolean prettyFlag;
	
	@JsonIgnore
	String edition;
	
	@JsonIgnore
	String availability;
	
	@JsonIgnore
	String xmlFileName;					//church-allen-allen.xml
	
	@JsonIgnore
	String transformedTextFileName;		//church-allen-allen.txt
	
	@JsonIgnore
	String jsonFileName;				//church-allen-allen.json
	

	/**
	 * ArkId: http://docsouth.unc.edu/church/negrochurch/menu.html
	 * @param collectionid
	 * @param titleid
	 */
	public V2DocSouthDocument(String collectionid, String titleid) {
		collectionId = "DocSouth";
		setProvider("docsouth");
		setDocsouthCollId(collectionid);
		setTitleId(titleid);
		//String url = "http://docsouth.unc.edu/" + getCollectionId() + "/" + getTitleId() + "/menu.html";   //xml filename has the correct address
		//setArkId(url);
		
		setInputDir( "input" + File.separator + "docsouth" );
		setOutputDir("output" + File.separator + "docsouth");
		
		setDocType("document"); 	//default type
		
		String collection = null;
		switch(collectionid) {
		case "fpn":
			collection="First-Person Narratives of the American South";
			break;
		case "church":
			collection="The Church in the Southern Black Community";
			break;
		case "southlit":
			collection="Library of Southern Literature";
			break;
		case "neh":
			collection="North American Slave Narratives";
			break;
		default:
		}
		setCollection(Arrays.asList(collection));
		
		
		
		
	}



	/**
	 * Retrieve metadata from TEI xml file
	 * @param xmlfilename ie church-allen-allen.xml, 
	 * @throws Exception 
	 */
	public void readMetadata(String xmlfilename) throws Exception {
		
		setXmlFileName(xmlfilename);
		
		Pattern p = Pattern.compile("^(.*)-(.*)-(.*)$");
		Matcher m = p.matcher(xmlfilename);
		if ( m.find() ) {
			String dir = m.group(1);
			String titleid = m.group(2);
			
			//church-negrochurch-dubois.xml --> http://docsouth.unc.edu/church/negrochurch/menu.html
			String url = "http://docsouth.unc.edu/" + dir + "/" + titleid + "/menu.html";
			setUrl(url);
			setArkId(url);
			
		}
		else {
			throw new Exception("Cannot parse xmlfilename " + xmlfilename);
		}
		

		
		String teiXmlFullName = getInputDir() + File.separator + getDocsouthCollId() + File.separator + "data" + File.separator + "xml" + File.separator + xmlfilename;

		//parse xml file
		Document doc;
		try {
			doc = TDMUtil.parseXML( new File(teiXmlFullName) );
		} catch (ParserConfigurationException | SAXException | IOException e) {
			throw new Exception( teiXmlFullName + " cannot be parsed. " );
		}


		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList titlenodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/fileDesc/titleStmt/title",
				doc, XPathConstants.NODESET);

		if ( titlenodes != null && titlenodes.getLength() > 0 ) {
			Element e = (Element) titlenodes.item(0);
			String title = e.getTextContent().trim();
			setTitle(title);
		}

		xPath = XPathFactory.newInstance().newXPath();
		NodeList authornodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/fileDesc/titleStmt/author",
				doc, XPathConstants.NODESET);

		List<String> creators = new ArrayList<>();
		if ( authornodes != null && authornodes.getLength() > 0 ) {
			Element e = (Element) authornodes.item(0);
			String author = e.getTextContent().trim();
			creators.add(author);
		}
		setCreator(creators);
						
		
		xPath = XPathFactory.newInstance().newXPath();
		NodeList editionnodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/fileDesc/editionStmt/edition",
				doc, XPathConstants.NODESET);

		if ( editionnodes != null && editionnodes.getLength() > 0 ) {
			Element e = (Element) editionnodes.item(0);
			String edition = e.getTextContent().trim();
			setEdition(edition);
		}
		
		xPath = XPathFactory.newInstance().newXPath();
		NodeList publishernodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/fileDesc/publicationStmt/publisher",
				doc, XPathConstants.NODESET);

		if ( publishernodes != null && publishernodes.getLength() > 0 ) {
			Element e = (Element) publishernodes.item(0);
			String publisher = e.getTextContent().trim();
			setPublisher(publisher);
		}

		xPath = XPathFactory.newInstance().newXPath();
		NodeList pubplacenodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/fileDesc/publicationStmt/pubPlace",
				doc, XPathConstants.NODESET);

		if ( pubplacenodes != null && pubplacenodes.getLength() > 0 ) {
			Element e = (Element) pubplacenodes.item(0);
			String pubPlace = e.getTextContent().trim();
			setPlaceOfPublication(pubPlace);
		}
		
		xPath = XPathFactory.newInstance().newXPath();
		NodeList datenodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/fileDesc/publicationStmt/date",
				doc, XPathConstants.NODESET);

		if ( datenodes != null && datenodes.getLength() > 0 ) {
			Element e = (Element) datenodes.item(0);
			String year = e.getTextContent().trim();
			int yearInt = Integer.parseInt(year.replaceAll("\\D", ""));
			setPublicationYear(yearInt);
		}
		
		xPath = XPathFactory.newInstance().newXPath();
		NodeList availabilitynodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/fileDesc/publicationStmt/availability",
				doc, XPathConstants.NODESET);

		if ( availabilitynodes != null && availabilitynodes.getLength() > 0 ) {
			Element e = (Element) availabilitynodes.item(0);
			String availability = e.getTextContent().trim();
			setAvailability(availability);
		}
		
		
		//lccn: Library of Congress control number, not call number
		/*xPath = XPathFactory.newInstance().newXPath();
		NodeList callnonodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/fileDesc/notesStmt/note",
				doc, XPathConstants.NODESET);
		List<Identifier> idList = new ArrayList<>();
		if ( callnonodes != null && callnonodes.getLength() > 0 ) {
			Element e = (Element) callnonodes.item(0);
			String callno = e.getTextContent().trim();
			Identifier id = new Identifier("lccn", callno);
			idList.add(id);
		}
		setIdentifiers(idList);*/
		
		xPath = XPathFactory.newInstance().newXPath();
		NodeList subjectnodes = (NodeList)xPath.evaluate("/TEI.2/teiHeader/profileDesc/textClass/keywords/list/item",
				doc, XPathConstants.NODESET);
		List<String> subjectList = new ArrayList<>();
		
		for (int i = 0; i < subjectnodes.getLength(); i++) {
			Element e = (Element) subjectnodes.item(i);
			String subject = e.getTextContent().trim();
			
			subjectList.add(subject);
		}
		setSourceCategory(subjectList);

		//see if there are chapters inside
		xPath = XPathFactory.newInstance().newXPath();
		NodeList divnodes = (NodeList)xPath.evaluate("/TEI.2/text/body//div1[@type!='main'] | /TEI.2/text/body//div2",
				doc, XPathConstants.NODESET);
		List<String> divhead = new ArrayList<>();
		int chapterCount = 0;
		for (int i = 0; i < divnodes.getLength(); i++) {
			Element e = (Element) divnodes.item(i);
			xPath = XPathFactory.newInstance().newXPath();
			NodeList headnodes = (NodeList)xPath.evaluate("head", e, XPathConstants.NODESET);
			String head = "";
			for(int j=0; j< headnodes.getLength(); j++){
				Element head_e = (Element) headnodes.item(j);
				String head_str = head_e.getTextContent().trim();
				
				
				if ( head_str.toLowerCase().indexOf("chapter") != -1 ) {
					chapterCount++;
				}
				
				head += head_str + " ";
				
			}
			if ( ! head.isEmpty()) {
				divhead.add(head);
			}
		}
		
		setHasPartTitle(divhead);
		
		if ( chapterCount> 3) {
			setDocType("book"); 
		}
	}
	


	public void transformXml2Txt() throws Exception {
		
		String xmlfilename = getXmlFileName();
		String textfilename = xmlfilename.replace("xml", "txt");
		
		setTransformedTextFileName(textfilename);
		
		String inputXmlFile = getInputDir() + File.separator + getDocsouthCollId() + File.separator + "data" + File.separator + "xml" + File.separator + xmlfilename;
		String outputFile = getOutputDir() + File.separator + getDocsouthCollId() + File.separator + "transformed_text" + File.separator + textfilename;
		String xslFile = getInputDir() + File.separator + getDocsouthCollId() + File.separator + "data" + File.separator + XSLFILENAME;
		
		try {
			TDMUtil.transformXml(inputXmlFile, outputFile, xslFile);
		}
		catch(Exception e ) {
			logger.error( programName + ":transformXml2Txt :Error transforming " + xmlfilename + " to " + textfilename + " " + e.getMessage() );
			throw e;
		}
		
	}


	public void getFullTextFromTEIXml() throws IOException {
		
		String xmlfilename = getXmlFileName();
		String textfilename = xmlfilename.replace("xml", "txt");
		
		setTransformedTextFileName(textfilename);
		
		String inputXmlFile = getInputDir() + File.separator + getDocsouthCollId() + File.separator + "data" + File.separator + "xml" + File.separator + xmlfilename;
		String outputFile = getOutputDir() + File.separator + getDocsouthCollId() + File.separator + "transformed_text" + File.separator + textfilename;
		
		Path xmlpath = FileSystems.getDefault().getPath( inputXmlFile );
		Path txtpath = Paths.get(outputFile);
		
		String fileContent = "";
		try {
			fileContent = new String(Files.readAllBytes(xmlpath), StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error( programName + ":getFullTextFromTEIXml :read file content from " + inputXmlFile + " " + e.getMessage());
			throw e;
		}
		
		//only get <text> content
		Pattern p1 = Pattern.compile("<text>(.*)</text>", Pattern.CASE_INSENSITIVE|Pattern.DOTALL|Pattern.MULTILINE);
		Matcher m1 = p1.matcher(fileContent);
		if ( m1.find()) {
			fileContent = m1.group(1);
		}
		
		//replace all <pb> tag with PAGEBREAK
		fileContent = fileContent.replaceAll("<pb[^>]*n=\"(.*)\"/>", "PAGEBREAK");
		//other possible page breaks
		fileContent = fileContent.replaceAll("</div1>", "PAGEBREAK");
		fileContent = fileContent.replaceAll("</titlePage>", "PAGEBREAK");
		fileContent = fileContent.replaceAll("<body>", "PAGEBREAK");
		fileContent = fileContent.replaceAll("<trailer>", "PAGEBREAK");
		
		
		//remove all tags
		fileContent = fileContent.replaceAll("<[^>]+>", "");
		
		fileContent = TDMUtil.dehypendateText( fileContent );
		
		//System.out.println(newContent);
		
		try {
			Files.write(txtpath, fileContent.getBytes());
		} catch (IOException e) {
			logger.error( programName + ":getFullTextFromTEIXml :write to text file " + outputFile + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		
		
	}


	/**
	 * Use transformed text file (with PAGEBREAK) to split text into pages
	 * @throws IOException 
	 */
	public void performNLPTasks() throws IOException {
		
	    List<String> outputformat = new ArrayList<>();
	    outputformat.add("unigram");
	    
		List<String> pageTextList = null;
		try {
			pageTextList = getPageTextList();
			
			if ( pageTextList != null ) {
				setFullText( pageTextList );
				
				int pageCount = pageTextList.size();
				setPageCount(pageCount);
				
				outputformat.add("fullText");
			}
			
			
		} catch (IOException e) {
			logger.error( programName + ":performNLPTasks: getPageTextList: " + getDocsouthCollId() + " " + getTitleId() + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
		setOutputFormat(outputformat);
		
		//join page text into 1 string
		String fulltext = String.join("\n", pageTextList); 

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
		//Map<String, Integer> wordCountMap = TDMUtil.generateUnigramMapFromWords( words );
		//setUnigramCount(wordCountMap);
		 
		
		
	}


	private List<String> getPageTextList() throws IOException {
		
		String textfilename = getTransformedTextFileName();
		String textfileWithPath = getOutputDir() + File.separator + getDocsouthCollId() + File.separator + "transformed_text" + File.separator + textfilename;
		
		
		//read in whole file content
		Path path = FileSystems.getDefault().getPath( textfileWithPath );
		String fileContent = "";
		try {
			fileContent = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
		} catch (IOException e) {
			//e.printStackTrace();
			throw e;
		}
		
		//split 
		List<String> pageTextList = new ArrayList<>();
		String[] texts = fileContent.split("PAGEBREAK");
		pageTextList = Arrays.asList(texts);
		
		return pageTextList;
	}



	public void logToDB() {
		// TODO Auto-generated method stub
		
	}


	/**
	 * Return created json filename with path
	 * @return
	 * @throws IOException
	 */
	public String outputJson() throws IOException {
		
		String outputDir = getOutputDir();
		String jsonStr = toJsonStr( prettyFlag );

		String jsonfilename = getXmlFileName().replace("xml", "json");		
        List<String> lines = Arrays.asList(jsonStr);
        
        Path dirPathObj = Paths.get( outputDir + File.separator + getDocsouthCollId()   );

        if( ! Files.exists(dirPathObj)) {
            
            try {
                // Creating The New Directory Structure
                Files.createDirectories(dirPathObj);
                
            } catch (IOException ioExceptionObj) {
            	logger.error( programName + ":outputJson:createDirectories: Error creating directory " + dirPathObj.getFileName() + " " + ioExceptionObj.getMessage());
                throw ioExceptionObj;

            }
        }
        
        Path file = Paths.get( getOutputDir() + File.separator + getDocsouthCollId() + File.separator + jsonfilename );
        
        try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			logger.error( programName + ":outputJson: Error writing to " + file.toString() + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
        
        return file.toString();
		
	}



	private String toJsonStr(boolean pretty_flag) {
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



	public String getCollectionId() {
		return collectionId;
	}

	public void setCollectionId(String collectionId) {
		this.collectionId = collectionId;
	}

	public String getDocsouthCollId() {
		return docsouthCollId;
	}

	public void setDocsouthCollId(String docsouthCollId) {
		this.docsouthCollId = docsouthCollId;
	}

	public String getTitleId() {
		return titleId;
	}

	public void setTitleId(String titleid) {
		this.titleId = titleid;
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


	public String getEdition() {
		return edition;
	}


	public void setEdition(String edition) {
		this.edition = edition;
	}


	public String getAvailability() {
		return availability;
	}


	public void setAvailability(String availability) {
		this.availability = availability;
	}



	public String getXmlFileName() {
		return xmlFileName;
	}



	public void setXmlFileName(String xmlFileName) {
		this.xmlFileName = xmlFileName;
	}



	public String getTransformedTextFileName() {
		return transformedTextFileName;
	}



	public void setTransformedTextFileName(String transformedTextFileName) {
		this.transformedTextFileName = transformedTextFileName;
	}



	public String getJsonFileName() {
		return jsonFileName;
	}



	public void setJsonFileName(String jsonFileName) {
		this.jsonFileName = jsonFileName;
	}




}

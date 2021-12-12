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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.portico.tdm.tdm2.datawarehouse.DWArticle;
import org.portico.tdm.tdm2.datawarehouse.Identifier;
import org.portico.tdm.tdm2.tools.TDMUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;


/**
 * 12/8/2021 Added docSubType in TDMV2JsonObject and V2Article
 * @author dxie
 *
 */
public class V2Article extends TDMV2JsonObject {
	
	@JsonIgnore
	static Logger logger = LogManager.getLogger(V2Article.class.getName());
	@JsonIgnore
	static String programName = "V2Article";
	
	@JsonIgnore
	DWArticle dw_article;
	
	@JsonIgnore
	String subDir;
	
	@JsonIgnore
	String publisherId;
	
	@JsonIgnore
	String outputDir;
	
	@JsonIgnore
	String inputDir;
	
	@JsonIgnore
	boolean prettyFlag;
	
	@JsonIgnore
	String pdfFile;
	
	@JsonIgnore
	String xmlFile;
	
	@JsonIgnore
	String fulltextFrom;				//"pdf", "xml", "html", "xml header", "no fulltext", "AU does not exist"
	
	@JsonIgnore
	String article_type_in_xml;			//retrieved from JATS xml file, will be saved in tdm_au table

	
	public V2Article(String au_id) {
		
		super();
		
		setAuid(au_id);
		setDocType("article");
		setProvider("portico");
		
		prettyFlag = false;
		setOutputFormat(new ArrayList<String>(Arrays.asList("unigram")) );
		
		
		
	}

	/**
	 * Retrieve metadata info from DWArticle object.
	 * @param article
	 */
	public void createV2ObjectFromDWArticle(DWArticle article) {
		
		setDw_article(article);
		
		String csname = article.getContent_set_name();
		setContentSetName(csname);

		String arkId = article.getAu_id().replace("ark:/", "ark://");
		setArkId( arkId);
		
		String article_title = article.getArticle_title();
		setTitle(article_title);
		
		String pub_year = article.getPub_year();
		try{
			int year = Integer.parseInt(pub_year);
			setPublicationYear(year);
		}
		catch(Exception e) {
			logger.error( programName + ":createV2ObjectFromDWArticle: wrong pub_year " + pub_year);
		}
		
		String doi = article.getDoi();
		if ( doi != null ) {
			setDoi(doi);
			if ( doi.startsWith("http")) {
				setUrl( doi );
			}
			else {
				setUrl( "http://doi.org/" + doi );
			}
		}
		
		Date datePublished = article.getPub_date();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String datePublishedStr = dateFormat.format(datePublished);
		setDatePublished(datePublishedStr);
		
		//process all identifiers of article
		processAndSetIdentifiers();
		
		String article_creators = article.getArticle_creator();
		List<String> creators = TDMUtil.convertPMDCreatorsToNameList( article_creators );
		setCreator( creators );
		
		String start_page_no = article.getStart_page_no();
		if ( start_page_no != null ) {
			setPageStart( start_page_no);
		}
		
		String end_page_no = article.getEnd_page_no();
		if ( end_page_no != null ) {
			setPageEnd( end_page_no );
		}
		
		String page_range = article.getPage_ranges();
		if ( page_range != null ) {
			setPagination( page_range);
		}
		
		String issue_no = article.getIssue_no();
		setIssueNumber( issue_no );
		
		String vol_no = article.getVol_no();
		setVolumeNumber( vol_no );
		
		//CoreNLP doesn't have language detection, use Article metadata for now
		String language = article.getLanguage();		//could be null
		String lang3letter = TDMUtil.convertLan2ToLan3( language );
		
		if ( lang3letter != null ) {
			setLanguage(  new ArrayList<String>(Arrays.asList(lang3letter)) );
		}

	}
	
	

	private void processAndSetIdentifiers() {
		
		List<Identifier> idList = new ArrayList<>();
		DWArticle article = getDw_article();
		
		String oclc = article.getOclc();
		String pii = article.getPii();
		String url = article.getUrl();
		String isbn = article.getIsbn();
		List<String> issn = article.getIssn();
		String doi = article.getDoi();
		String publisherid = article.getInstitution_article_id();
		String csname = article.getContent_set_name();
		String journal_arkid = article.getJournal_ark_id();
		String issue_arkid = article.getIssue_ark_id();
		
		if ( oclc != null ) {
			try {
				Identifier id = new Identifier( "oclc", oclc);
				idList.add(id);
				
			} catch (Exception e) {
				logger.error( programName + ":processAndSetIdentifiers: Error generating oclc Identifier from " + oclc + " " + e.getMessage());
			}
		}
		
		if ( pii != null ) {
			try {
				//Identifier id = new Identifier("pii", pii);
				Identifier id = new Identifier("local_publisher_id", pii);
				idList.add(id);
			} catch (Exception e) {
				logger.error( programName + ":processAndSetIdentifiers: Error generating local_publisher_id(pii) Identifier from " + pii + " "  + e.getMessage());
			}
		}
		
		/* 8/31/2021 For privacy purpose take out PII
		 * if ( url != null ) { try { Identifier id = new Identifier("url", url);
		 * idList.add(id); } catch (Exception e) { logger.error( programName +
		 * ":processAndSetIdentifiers: Error generating url Identifier from " + url +
		 * " " + e.getMessage()); } }
		 */
	
		if ( isbn != null ) {
			try {
				Identifier id = new Identifier("isbn", isbn); 	//could contain multiple isbn with ,
				idList.add(id);
			} catch (Exception e) {
				logger.error( programName + ":processAndSetIdentifiers: Error generating isbn Identifier from " + isbn + " " + e.getMessage());
			}
		}
		
		if ( doi != null ) {
			try {
				Identifier id = new Identifier("doi", doi);
				idList.add(id);
			} catch (Exception e) {
				logger.error( programName + ":processAndSetIdentifiers: Error generating doi Identifier from " + doi + " " + e.getMessage());
			}
		}
		
		if ( issn != null && !issn.isEmpty()) {
			for(String issnStr: issn ) {
				try {
					Identifier id = new Identifier("issn", issnStr);
					idList.add(id);
				} catch (Exception e) {
					logger.error( programName + ":processAndSetIdentifiers: Error generating issn Identifier from " + issnStr + " "  + e.getMessage());
				}
			}
		}
		
		if ( publisherid != null && !publisherid.isEmpty()) {
			
				try {
					Identifier id = new Identifier("local_publisher_id", publisherid);
					idList.add(id);
				} catch (Exception e) {
					logger.error( programName + ":processAndSetIdentifiers: Error generating publisherid Identifier from " + publisherid + " "  + e.getMessage());
				}

		}
		
		Identifier csid;
		try {
			csid = new Identifier("local_content_set", csname );
			idList.add(csid);

		} catch (Exception e) {
			logger.error( programName + ":processAndSetIdentifiers: Error generating content set name Identifier from " + csname + " "  + e.getMessage());
		}
		
		if ( journal_arkid != null && ! journal_arkid.isEmpty()) {
			try {
				Identifier id = new Identifier("journal_id", journal_arkid);
				idList.add(id);
			} catch (Exception e) {
				logger.error( programName + ":processAndSetIdentifiers: Error generating journal ark id Identifier from " + journal_arkid + " "  + e.getMessage());
			}
		}
		
		if ( issue_arkid != null && ! issue_arkid.isEmpty()) {
			try {
				Identifier id = new Identifier("issue_id", issue_arkid);
				idList.add(id);
			} catch (Exception e) {
				logger.error( programName + ":processAndSetIdentifiers: Error generating issue ark id Identifier from " + issue_arkid + " "  + e.getMessage());
			}
		}
		
		setIdentifiers(idList);
				
	}


	public void performNLPTasks() throws Exception {
		
		DWArticle article = getDw_article();
		String csname = article.getContent_set_name();
		String volNo = article.getVol_no();
		String issueNo = article.getIssue_no();
		String au_id = article.getAu_id();
		String subDir = getSubDir();
		
		//get full text by page
		List<String> articlePageTextList = new ArrayList<>();
		try {
			//Return split page texts of an article. End of line hyphens have been removed.
			articlePageTextList = splitArticleByPages(csname, au_id, subDir );  
			
			if ( articlePageTextList != null ) {
				setFullText( articlePageTextList );
				
				int articlePageCount = articlePageTextList.size();
				setPageCount(articlePageCount);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error( programName + ":performNLPTasks: splitArticleByPages " + csname + " v." + volNo + " n." + issueNo + ": " + au_id + " " + e.getMessage());
			throw e;
		}

		//join page text into 1 string
		String fulltext = String.join("\n", articlePageTextList); 
		
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
 
		//abstract
        //TODO: Not in DB. Need to read xml file to get abstract
		
		//sourceCategory
        //TODO: also in xml file
		
	}
	
	
	public void retrieveArticleTypeFromJatsXml() throws Exception {
		
		String subdir = getSubDir();
		String inputdir = getInputDir();
		String auid = getAuid();
		String csname = getDw_article().getContent_set_name();
		
		String bitsXmlFileName = null;
		try {
			bitsXmlFileName = TDMUtil.getBitsXmlFileNameForAU(auid);
		} catch (Exception e) {
			logger.error( programName + ":retrieveArticleTypeFromJatsXml: Error getting BITS xml file for " + auid);
			e.printStackTrace();
			throw e;
		}
		
		if ( bitsXmlFileName == null ) {
			logger.error( programName + ":retrieveArticleTypeFromJatsXml: AU not exist");
			throw new Exception("No Bits XML file");
		}
		
		//constructs full xml file name , ie input/academicus/ISSN_20793715/phz937z4sqq/data/
		String fullBitsXmlFileName = inputdir + File.separatorChar + subdir +  File.separator + csname + File.separator + auid.replace("ark:/27927/", "") 
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
			logger.info(programName + ":retrieveArticleTypeFromJatsXml" + auid + " null article-type attribute" );
		}
        
		setArticle_type_in_xml(article_type);
		
		setDocSubType(article_type);
	}




	/**
	 * Get split article full text by page in String list. To get full text, check in this order: pdf, xml full text, html, xml header file.
	 *  If all fails, throw Exception
	 * @param csname
	 * @param au_id
	 * @param subDir
	 * @return
	 * @throws Exception
	 */
	private List<String> splitArticleByPages(String csname, String au_id, String subDir) throws Exception  {
		
		List<String> pageTextList = null;

		String publisher = getPublisherId();
		String volno = getVolumeNumber();
		String issueno = getIssueNumber();
		
		
		try {
			pageTextList = splitArticleByPageFromPdf( csname, au_id, subDir ); //SU: pmd_status='Active', a_content_function='Rendition: Page Images', pmd_mime_type='application/pdf'
			
			if ( pageTextList != null ) {
				//logger.info( programName + ":splitArticleByPages :splitArticleByPageFromHtml : " + publisher + " " + csname + "\t" + volno + "\t" + issueno + " " + au_id + ": Used pdf file for fulltext");
				setFulltextFrom("pdf");
				return pageTextList;
			}
			
		} catch (Exception e1) {
			//logger.info( programName + ":splitArticleByPages :splitArticleByPageFromPdf : " + csname + "\t" + volno + "\t" + issueno + " " + au_id + ": " + e1.getMessage());
			if ( e1.getMessage().indexOf("No such file or directory") != -1 ) {
				setFulltextFrom("AU does not exist");
				throw e1;
			}
		}
		
		try {
			pageTextList = splitArticleByPageFromXml( csname, au_id, subDir );//SU: pmd_status='Active', a_content_function='Text: Marked Up Full Text', pmd_mime_type='application/xml'
			
			if ( pageTextList != null ) {
				//logger.info( programName + ":splitArticleByPages :splitArticleByPageFromHtml : " + publisher + " " + csname + "\t" + volno + "\t" + issueno + " " + au_id + ": Used xml file for fulltext");
				setFulltextFrom("xml");
				return pageTextList;
			}
			
		} catch (Exception e1) {
			//logger.info( programName + ":splitArticleByPages :splitArticleByPageFromXml : " + csname + "\t" + volno + "\t" + issueno + " " + au_id + ": " + e1.getMessage());
		}
		
		try {
			pageTextList = splitArticleByPageFromHtml( csname, au_id, subDir );//SU: pmd_status='Active', pmd_mime_type='text/html' and a_content_function='Rendition: Web'
			
			if ( pageTextList != null ) {
				//logger.info( programName + ":splitArticleByPages :splitArticleByPageFromHtml : " + publisher + " " + csname + "\t" + volno + "\t" + issueno + " " + au_id + ": Used html file for fulltext");
				setFulltextFrom("html");
				return pageTextList;
			}
			
		} catch (Exception e1) {
			//logger.info( programName + ":splitArticleByPages :splitArticleByPageFromHtml : " + csname + "\t" + volno + "\t" + issueno + " " + au_id + ": " + e1.getMessage());
		}
		
		try {
			pageTextList = splitArticleByPageFromXmlHeaderFile( csname, au_id, subDir );//SU: pmd_status='Active', a_content_function='Text: Marked Up Header', pmd_mime_type='application/xml'
			
			if ( pageTextList != null ) {
				//logger.info( programName + ":splitArticleByPages :splitArticleByPageFromHtml : " + publisher + " " + csname + "\t" + volno + "\t" + issueno + " " + au_id + ": Used xml header file for fulltext");
				setFulltextFrom("xml header");
				return pageTextList;
			}
			
		} catch (Exception e1) {
			//logger.info( programName + ":splitArticleByPages :splitArticleByPageFromXmlHeaderFile : " + csname + "\t" + volno + "\t" + issueno + " " + au_id + ": " + e1.getMessage());
		}
		
		logger.error( programName + ":splitArticleByPages :Cannot get fulltext for : " + publisher + " " + csname + "\t" + volno + "\t" + issueno + " " + au_id );
		setFulltextFrom("no fulltext");
		
		throw new Exception("Cannot get fulltext for " + publisher + " " + csname + "\t" + volno + "\t" + issueno + " " + au_id);

	}


	
	private List<String> splitArticleByPageFromXmlHeaderFile(String csname, String au_id, String subdir) throws Exception {
		
		List<String> pageTextList = new ArrayList<>();

		String volno = getVolumeNumber();
		String issueno = getIssueNumber();
		String suXmlFile = null;
		
		try {
			suXmlFile = TDMUtil.findActiveXMLHeaderSUOfAU( au_id );   //Find a SU: pmd_status='Active', a_content_function='Text: Marked Up Header', pmd_mime_type='application/xml'
			
		} catch (Exception e1) {
			//logger.error( programName + ":splitArticleByPageFromXmlHeaderFile :findActiveXMLSUOfAU : " + csname + "\t" + volno + "\t" + issueno + " " + au_id  + " " + e1.getMessage());
			throw e1;
		}
		
		if ( suXmlFile == null ) {
			throw new Exception("Cannot find active xml su file");
		}
		
		setXmlFile(suXmlFile);
		
		String xmlFileWithPath = getInputDir() + File.separator + subdir + File.separator + csname + File.separator 
									+ au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + suXmlFile;
		
		String fulltextFromXml = null;
		try {
			fulltextFromXml = TDMUtil.getFullTextFromXMLFile( xmlFileWithPath );
		} catch (Exception e) {
			logger.error( programName + ":splitArticleByPageFromXmlHeaderFile :getFullTextFromXMLFile for " + csname + "\t" + volno + "\t" + issueno + " " + au_id + " " 
							+ " from " + xmlFileWithPath +  " " + e.getMessage());
			throw new Exception("Cannot get full text from xml file");
		}
		
		if ( fulltextFromXml != null ) {
			pageTextList = TDMUtil.manaulSplitTextToPages(csname, au_id, fulltextFromXml);
		}
		else {
			logger.error( programName + ":splitArticleByPageFromXmlHeaderFile :manaulSplitTextToPages : " + csname + "\t" + volno + "\t" + issueno + " " + au_id  );
			throw new Exception("Cannot get full text from xml file");
		}
		
		return pageTextList;
	}


	/**
	 * Use PDF file to get fulltext, split by pages. If anything wrong happens, throw Exception.
	 * @param csname
	 * @param au_id
	 * @param subdir
	 * @return
	 * @throws Exception 
	 */
	private List<String> splitArticleByPageFromPdf(String csname, String au_id, String subdir ) throws Exception {
		
		List<String> pageTextList = new ArrayList<>();
		
		String pdfFile = null;
		String volno = getVolumeNumber();
		String issueno = getIssueNumber();
		
		
		try {
			pdfFile = TDMUtil.findPDFFileName(au_id);    //Find a SU: pmd_status='Active', a_content_function='Rendition: Page Images', pmd_mime_type='application/pdf'
					
		} catch (Exception e) {
			//logger.error( programName + ":splitArticleByPageFromPdf :findPDFFileName : " + csname + "\t" + volno + "\t" + issueno + " " + au_id + " " + e.getMessage());
			throw e;
		}
		
		setPdfFile(pdfFile);
	
		//Use pdf to get full text
		String pdfFileWithPath = inputDir + File.separator + subdir + File.separator +  csname + File.separator 
					+ au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + pdfFile;

		
		//split pdf file into pages using pdfbox tool
		PDDocument document = null;
		try {
			document = PDDocument.load(new File(pdfFileWithPath)); 
			Splitter splitter = new Splitter();
			try {
				List<PDDocument> splittedDocuments = splitter.split(document);

				for(PDDocument doc: splittedDocuments) {
					try {
						String text = new PDFTextStripper().getText(doc);

						//remove end of line hyphens
						text = TDMUtil.dehypendateText(text);
						//remove non UTF-8 characters
						text = TDMUtil.normalizeUTF8String(text);
						//System.out.println(text);

						pageTextList.add(text);
					} catch (Exception e3) {
						logger.error( programName + ":splitArticleByPageFromPdf : Error get text from split document : " + csname + "\t" + volno + "\t" + issueno + " " + au_id + " " + e3.getMessage());
						throw e3;
					}


				}

			} catch (Exception e3) {
				logger.error( programName + ":splitArticleByPageFromPdf : Error split document with Splitter : " + csname + "\t" + volno + "\t" + issueno + " " + au_id + " " + e3.getMessage());
				throw e3;
			}
			finally {
				document.close();
			}

		} catch (Exception e3) {
			//logger.error( programName + ":splitArticleByPageFromPdf : Error loading document with pdf box : " + csname + "\t" + volno + "\t" + issueno + " " + au_id  + " " + e3.getMessage());
			throw e3;
		}
		finally
		{
		   if( document != null )
		   {
			   try {
				document.close();
			} catch (IOException e) {
				
			}
		   }
		}
		
		return pageTextList;
	}


	/**
	 * Use XML to get full text. If fails, throw Exception.
	 * @param csname
	 * @param au_id
	 * @param subdir
	 * @return
	 * @throws Exception
	 */
	private List<String> splitArticleByPageFromXml(String csname, String au_id, String subdir) throws Exception {
		
		List<String> pageTextList = new ArrayList<>();

		String volno = getVolumeNumber();
		String issueno = getIssueNumber();
		String suXmlFile = null;
		
		try {
			suXmlFile = TDMUtil.findActiveXMLSUOfAU( au_id );   //Find a SU: pmd_status='Active', a_content_function='Text: Marked Up Full Text', pmd_mime_type='application/xml'
			
		} catch (Exception e1) {
			//logger.info( programName + ":splitArticleByPageFromXml :findActiveXMLSUOfAU : " + csname + "\t" + volno + "\t" + issueno + " " + au_id  + ": " + e1.getMessage());
			throw e1;
		}
		
		if ( suXmlFile == null ) {
			throw new Exception("Cannot find active xml su file");
		}
		
		setXmlFile(suXmlFile);
		
		String xmlFileWithPath = getInputDir() + File.separator + subdir + File.separator + csname + File.separator 
									+ au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + suXmlFile;
		
		String fulltextFromXml = null;
		try {
			fulltextFromXml = TDMUtil.getFullTextFromXMLFile( xmlFileWithPath );
		} catch (Exception e) {
			logger.error( programName + ":splitArticleByPageFromXml :getFullTextFromXMLFile for " + csname + "\t" + volno + "\t" + issueno + " " + au_id + " " 
							+ " from " + xmlFileWithPath +  " " + e.getMessage());
			throw new Exception("Cannot get full text from xml file");
		}
		
		if ( fulltextFromXml != null ) {
			pageTextList = TDMUtil.manaulSplitTextToPages(csname, au_id, fulltextFromXml);
		}
		else {
			logger.error( programName + ":splitArticleByPageFromXml :manaulSplitTextToPages : " + csname + "\t" + volno + "\t" + issueno + " " + au_id  );
			throw new Exception("Cannot get full text from xml file");
		}
		
		return pageTextList;
	}


	private List<String> splitArticleByPageFromHtml(String csname, String au_id, String subdir) throws Exception {
		
		List<String> pageTextList = new ArrayList<>();

		String volno = getVolumeNumber();
		String issueno = getIssueNumber();
		List<String> suHtmlFiles = null;
		
		try {
			suHtmlFiles = TDMUtil.findActiveHTMLOfAU( au_id );   //Find a SU: pmd_status='Active', a_content_function='Text: Marked Up Full Text', pmd_mime_type='application/xml'
			
		} catch (Exception e1) {
			//logger.info( programName + ":splitArticleByPageFromHtml :findActiveXMLSUOfAU : " + csname + "\t" + volno + "\t" + issueno + " " + au_id  + ": " + e1.getMessage());
			throw e1;
		}
		
		if ( suHtmlFiles == null ) {
			throw new Exception("Cannot find active html su file");
		}
		
		//setXmlFile(suHtmlFiles);
		
		for ( String suHtmlFile: suHtmlFiles) {
			String xmlFileWithPath = getInputDir() + File.separator + subdir + File.separator + csname + File.separator 
					+ au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + suHtmlFile;

			String fulltextFromXml = null;
			try {
				fulltextFromXml = TDMUtil.getFullTextFromXMLFile( xmlFileWithPath );
			} catch (Exception e) {
				logger.error( programName + ":splitArticleByPageFromHtml :getFullTextFromXMLFile for " + csname + "\t" + volno + "\t" + issueno + " " + au_id + " " 
						+ " from " + xmlFileWithPath +  " " + e.getMessage());
				throw new Exception("Cannot get full text from xml file");
			}

			if ( fulltextFromXml != null ) {
				pageTextList.addAll(TDMUtil.manaulSplitTextToPages(csname, au_id, fulltextFromXml));
			}
			else {
				logger.error( programName + ":splitArticleByPageFromHtml :manaulSplitTextToPages : " + csname + "\t" + volno + "\t" + issueno + " " + au_id  );
				throw new Exception("Cannot get full text from xml file");
			}

		}
		
		return pageTextList;
	}


	/**
	 * Log into TDM_AU table
	 * 
	 * 
	 */
	public void logToDB() {
		
		DWArticle article = getDw_article();
		String csname = article.getContent_set_name();
		String volNo = article.getVol_no();
		String issueNo = article.getIssue_no();
		String au_id = article.getAu_id();
		String publisherid = getPublisherId();
		String journal_arkid = article.getJournal_ark_id();
		String issue_arkid = article.getIssue_ark_id();
		String pub_year = article.getPub_year();
		String pdfFile = getPdfFile();
		String xmlFile = getXmlFile();
		String oa = "";
		String articleType = getArticle_type_in_xml();
		

		Calendar c = Calendar.getInstance(); 
		int this_month = c.get(Calendar.MONTH) + 1; // beware of month indexing from zero
		int this_year  = c.get(Calendar.YEAR);
		
		String cacheDir = "portico_cache_" + this_year + ( this_month<10? "0" + this_month: "" + this_month ) ;
		
		if ( isFullTextAvailable() ) {
			oa = "Y";
		}
		else {
			oa = "N";
		}
		
		int year = 0;
		try{
			year = Integer.parseInt(pub_year);
		}
		catch(Exception e) {
			logger.error( programName + ":logToDB: wrong pub_year " + pub_year);
		}
		
		String existance_query = "select count(*) from tdm_au where au_id='" + au_id + "'";
		
		String insert_query = "insert into TDM_AU ( au_id, publisher_id, type, content_set_name, vol_no, issue_no, "
				+ " journal_arkid, issue_arkid, pdf_file, xml_file, oa, pub_year, creation_ts, article_type, cache_dir) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
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
				insert_stmt.setString(2, publisherid);
				insert_stmt.setString(3, "article");
				insert_stmt.setString(4, csname);
				insert_stmt.setString(5, volNo);
				insert_stmt.setString(6, issueNo);
				insert_stmt.setString(7, journal_arkid);
				insert_stmt.setString(8, issue_arkid);
				insert_stmt.setString(9, pdfFile);
				insert_stmt.setString(10, xmlFile);
				insert_stmt.setString(11,  oa);
				insert_stmt.setInt(12, year);
				Timestamp sqlDate = new java.sql.Timestamp( new java.util.Date().getTime());
				insert_stmt.setTimestamp(13, sqlDate);
				insert_stmt.setString(14, articleType);
				insert_stmt.setString(15, cacheDir);

				insert_stmt.executeUpdate();

			}
			else {	//this au has been inserted before
				
				String update_query = "update tdm_au set publisher_id='" + publisherid + "', type='article', content_set_name=q'$" + csname + "$', "
						+ "vol_no=q'$" + volNo + "$', issue_no=q'$" + issueNo + "$', journal_arkid='" + journal_arkid + "', issue_arkid='" + issue_arkid + "', ";
				if ( pdfFile != null ) {
					update_query += "pdf_file='" + pdfFile + "', ";
				}
				if ( xmlFile != null ) {
					update_query += "xml_file='" + xmlFile + "', ";
				}
				if ( articleType != null ) {
					update_query += "article_type='" + articleType + "', cache_dir='" + cacheDir + "', ";
				}

				update_query += "oa='" + oa + "', pub_year=" + year + ", modification_ts=current_timestamp where au_id='" + au_id + "'";

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
		String csname = getContentSetName();
		String jsonStr = toJsonStr( prettyFlag );
		String auid = getAuid();
		String filename = auid.substring( auid.lastIndexOf("/") + 1) + ".json";		//ark:/27927/pgj2j7b70gv --> pgj2j7b70gv.json
        List<String> lines = Arrays.asList(jsonStr);
        
        Path dirPathObj = Paths.get( outputDir + File.separator + getSubDir() + File.separator + csname  );

        if( ! Files.exists(dirPathObj)) {
            
            try {
                // Creating The New Directory Structure
                Files.createDirectories(dirPathObj);
                
            } catch (IOException ioExceptionObj) {
            	logger.error( programName + ":outputJson:createDirectories: Error creating directory " + dirPathObj.getFileName() + " " + ioExceptionObj.getMessage());
                throw ioExceptionObj;

            }
        }
        
        Path file = Paths.get( outputDir + File.separator + getSubDir() + File.separator + csname + File.separator + filename );
        
        try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			logger.error( programName + ":outputJson: Error writing to " + file.toString() + " " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
	}
	
	
	/*
	 * This method adds "fullText" to outputFormat
	 */
	public void addOAToOutputFormat() {
		
		List<String> outputFormatList = getOutputFormat();
		if ( outputFormatList == null ) {
			outputFormatList = new ArrayList<>();
		}
		
		outputFormatList.add("fullText");
		setOutputFormat(outputFormatList);
		
		
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


	public DWArticle getDw_article() {
		return dw_article;
	}


	public void setDw_article(DWArticle dw_article) {
		this.dw_article = dw_article;
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


	public String getPdfFile() {
		return pdfFile;
	}


	public void setPdfFile(String pdfFile) {
		this.pdfFile = pdfFile;
	}


	public String getXmlFile() {
		return xmlFile;
	}


	public void setXmlFile(String xmlFile) {
		this.xmlFile = xmlFile;
	}


	public String getPublisherId() {
		return publisherId;
	}


	public void setPublisherId(String publisherId) {
		this.publisherId = publisherId;
	}


	public String getFulltextFrom() {
		return fulltextFrom;
	}


	public void setFulltextFrom(String fulltextFrom) {
		this.fulltextFrom = fulltextFrom;
	}

	public String getArticle_type_in_xml() {
		return article_type_in_xml;
	}

	public void setArticle_type_in_xml(String article_type_in_xml) {
		this.article_type_in_xml = article_type_in_xml;
	}


}

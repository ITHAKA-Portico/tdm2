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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.apache.pdfbox.pdmodel.interactive.action.PDActionGoTo;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDNamedDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDPageDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDDocumentOutline;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineItem;
import org.apache.pdfbox.text.PDFTextStripper;


import org.portico.tdm.tdm2.datawarehouse.Identifier;
import org.portico.tdm.tdm2.ebook.UniBook;
import org.portico.tdm.tdm2.schemaorg.Person;
import org.portico.tdm.tdm2.schemaorg.Publisher;
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


/**
 * If book has 1 single pdf, then create a book json file.
 * If book has pdf for each chapter, then only create chapter json files. Copy book's metadata into chapters when appropriate.
 * @author dxie
 *
 */
public class V2Book  extends TDMV2JsonObject {
	
	@JsonIgnore
	static Logger logger = LogManager.getLogger(V2Book.class.getName());
	@JsonIgnore
	static String programName = "V2Book";
	
	@JsonIgnore
	UniBook unibook;

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
	String bitsXmlFileSuId;
	
	@JsonIgnore
	boolean singlePdf;			//if true, it is one pdf for whole book. If false, it is pdf by chapters.
	
	@JsonIgnore
	List<V2Chapter> chapters; 
	
	@JsonIgnore
	List<String> pdfFiles;		//fulltext read from these pdfs
	
	@JsonIgnore
	List<String> xmlFiles;		//fulltext read from these xmls

	@JsonIgnore
	String fulltextFrom;		//"pdf", "xml", "html", "xml header", "no fulltext", "AU does not exist"
	
	@JsonIgnore
	int processedChapterCount;
	

	
	
	public V2Book(String au_id) throws Exception {
		
		super();
		
		setAuid(au_id);
		setArkId( au_id.replace("ark:/", "ark://") );
		setDocType("book");
		setProvider("portico");
		
		prettyFlag = false;
			
	}

	public V2Book(UniBook book) {
		
		super();
		
		this.unibook = book;
		
		setDocType("book");
		setProvider("portico");
		
		prettyFlag = false;
		//setOutputFormat(new ArrayList<String>(Arrays.asList("unigram")) );
		
	}
	
	public void populateUniBookFromDB() {
		// TODO Auto-generated method stub
		
	}


	public void populateV2BookFromUnibook() {
		// TODO Auto-generated method stub
		
	}
	
	
	/**
	 * For now, no book has OA 3/30/2020
	 */
	public void setBookOutputFormat() {

		List<String> outputFormat = new ArrayList<>();
		
		outputFormat.add("unigram");
		
		//check has_oa
/*		String publisherID = getPublisherId();

		try {
			boolean has_oa = TDMUtil.getOAStatusOfBookContentSet(publisherID);
			
			if ( has_oa) {
				outputFormat.add("fulltext");
				setFullTextAvailable( true );	//if it is OA title
			}
			else {
				setFullTextAvailable( false );	//if it is OA title
			}
			
		} catch (Exception e) {
			logger.error( programName + ":setBookOutputFormat " + publisherID + " " + e);
		}*/
		
		setOutputFormat(outputFormat);
		
	}


	
	
	/**
	 * Use A_SU table to get xml and pdf file names
	 * Use Bits xml file to get book metadata and Chapters
	 * @throws Exception 
	 */
	public void populatePorticoBookMetadata() throws Exception {
		
		String auid = getAuid();
		String subdir = getSubDir();
		
		//use DB table A_SU to get xml file name
		String bitsXmlFileName = null;
		try {
			bitsXmlFileName = TDMUtil.getBitsXmlFileNameForAU(auid);
		} catch (Exception e) {
			logger.error( programName + ":populatePorticoBookMetadata: Error getting BITS xml file for " + auid);
			e.printStackTrace();
			throw e;
		}
		
		if ( bitsXmlFileName == null ) {
			logger.error( programName + ":populatePorticoBookMetadata: Error getting BITS xml file for " + auid);
			throw new Exception("No bits xml file found");
		}
		
		logger.info( programName + ": populatePorticoBookMetadata : bitsXmlFile=" + bitsXmlFileName);
		setBitsXmlFileName(bitsXmlFileName);
		
		String bitsXmlFile_su_id = "ark:/27927/" + bitsXmlFileName.replace(".xml", "");
		setBitsXmlFileSuId(bitsXmlFile_su_id);
		
		//Read bits xml file and get book metadata
		try {
			populateBookMetadataFromXmlFile(auid, bitsXmlFileName, subdir);
		} catch (Exception e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile: " + auid + e.getMessage());
			throw e;
		}
		
	}
	
	

	private void populateBookMetadataFromXmlFile(String auid, String bitsXmlFileName, String subdir) throws Exception {
		
		//constructs full xml file name
		String fullBitsXmlFileName = "input" + File.separatorChar + "ebook" + File.separator + subdir +  File.separator + auid.replace("ark:/27927/", "") 
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
		
		//set publisher
		try {
			setBookPublisher(doc);
		} catch (Exception e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookPublisher for " + auid + " from bitsXmlFile " + e.getMessage());
		}
		
		//set title
		try {
			setBookTitle(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookTitle for " + auid + " from bitsXmlFile " + e.getMessage());
		}
		
		//set creator
		try {
			setBookCreator(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookCreator for " + auid + " from bitsXmlFile " + e.getMessage());
		}

		//set doi
		try {
			setBookDOI(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookDOI for " + auid + " from bitsXmlFile " + e.getMessage());
		}
		
		setBookURL(doc);

		
		//set isbn
		try {
			setBookISBN(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookISBN for " + auid + " from bitsXmlFile " + e.getMessage());
		}

		//set count
		try {
			setBookPageCount(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookPageCount for " + auid + " from bitsXmlFile " + e.getMessage());
		}
		
		//set pub year
		try {
			setBookPubDate(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookPubDate for " + auid + " from bitsXmlFile " + e.getMessage());
		} catch( IllegalArgumentException e) {
			if (e.getMessage().equals("Pub year 0")) {
				//we do not want to send books with pub year 0
				logger.error(programName + ":populateBookMetadataFromXmlFile  : pub year 0, stop processing " + auid );
				throw e;
			}
		}
		
		//set subject
		try {
			setBookSourceCategoery(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookSourceCategoery for " + auid + " from bitsXmlFile " + e.getMessage());
		}
	
		
		//set language
		try {
			setBookLanguage(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookLanguage for " + auid + " from bitsXmlFile " + e.getMessage());
		}
		
		//set abstrct
		try {
			setBookAbstract(doc);
		} catch (XPathExpressionException e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookEdition for " + auid + " from bitsXmlFile " + e.getMessage());
		}
		
		setIsPartOf(getContentSetName());
		
		//set pdfFiles
		try {
			setBookPdfs(doc);
		} catch (Exception e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error setBookPdfs for " + auid + " from bitsXmlFile " + e.getMessage());
			throw e;
		}
		
		//get all chapter info from bits xml file
		try {
			populateBookChapters(doc);
		} catch (Exception e) {
			logger.error( programName + ":populateBookMetadataFromXmlFile  :Error populateBookChapters for " + auid + " from bitsXmlFile " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Set pdfFiles if it is single book pdf. 
	 * Only Elgar, Benjamins and Sage books.
	 * @param doc
	 * @throws Exception
	 */
	private void setBookPdfs(Document doc) throws Exception {
		String providerID = getPublisherId();
		
		if ( providerID.equals("ELGAR")) {
			setElgarPdfs(doc);						//single pdf
		}
		else if ( providerID.equals("BENJAMINS")) {
			setBenjaminsPdfs(doc);					//single pdf
		}
		else if ( providerID.equals("SAGE")) {
			setSagePdfs(doc);					//single pdf
		}
		else if ( providerID.equals("CSIRO")) {
			setCSIROPdfs(doc);					//single pdf
		}
		/*else if ( providerID.equals("EUP")) {
			setEUPPdfs(doc);
		}
		else if ( providerID.equals("NOMOS")) {
			setNOMOSPdfs(doc);
		}
		else if ( providerID.equals("MUSE")) {
			setMUSEPdfs(doc);
		}
		else if ( providerID.equals("WILEY")) {
			setWileyPdfs(doc);
		}
		else if ( providerID.equals("RIA")) {
			setRIAPdfs(doc);
		}
		else if ( providerID.equals("THIEME")) {
			setThiemePdfs(doc);
		}
		else if ( providerID.equals("UCAL")) {
			setUCALPdfs(doc);
		}
		else if ( providerID.equals("CAMBRIDGE")) {
			setCAMBPdfs(doc);
		}
*/
	}
	
	private void setCSIROPdfs(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();
		
		int pdfFileCount = 0;
		
		try {
			pdfFileCount = TDMUtil.findPdfFileCountFromDB(bookAuId);
			if ( pdfFileCount == 1 ) {
				setSinglePdf(true);
			}
			
		} catch (Exception e3) {
			logger.error( programName + ":setCSIROPdfs: findPdfFileCountFromDB for " + bookAuId + " " + e3.getMessage());
			
		}
		
		//pdf file name is not stored in bits xml file. Get pdf file name from DB
		String pdf_archive = null;
		String su_id = null;

		if ( isSinglePdf() ) {
			try {
				su_id = TDMUtil.findSuidByAuid( bookAuId);			
			} catch (Exception e) {
				logger.error( programName + ":setCSIROPdfs :findSuidByAuid " + providerID + " " + bookAuId + " " + e);
				throw e;
			}


		}
		else {
			logger.error( programName + ":setCSIROPdfs More than 1 pdf file used for CSIRO's book!");
			throw new Exception("More than 1 pdf file used for CSIRO book");
		}


		if ( su_id != null && !su_id.isEmpty()) {
			pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
			pdfFiles.add(pdf_archive);
		}
		else if ( su_id == null ) {
			logger.error( programName + ":setCSIROPdfs: No su_id found for " + bookAuId );
			throw new Exception("No su_id found for book");
		}
		
	}

	private void setElgarPdfs(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();
		
		int pdfFileCount = 0;
		
		try {
			pdfFileCount = TDMUtil.findPdfFileCountFromDB(bookAuId);
			if ( pdfFileCount == 1 ) {
				setSinglePdf(true);
			}
			
		} catch (Exception e3) {
			logger.error( programName + ":setElgarPdfs: findPdfFileCountFromDB for " + bookAuId + " " + e3.getMessage());
			
		}
		
		//pdf file name is not stored in bits xml file. Get pdf file name from DB
		String pdf_archive = null;
		String su_id = null;

		if ( isSinglePdf() ) {
			try {
				su_id = TDMUtil.findSuidByAuid( bookAuId);			
			} catch (Exception e) {
				logger.error( programName + ":setElgarPdfs :findSuidByAuid " + providerID + " " + bookAuId + " " + e);
				throw e;
			}


		}
		else {
			logger.error( programName + ":processElgarBookChapters More than 1 pdf file used for Elgar's book!");
			throw new Exception("More than 1 pdf file used for Elgar book");
		}


		if ( su_id != null && !su_id.isEmpty()) {
			pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
			pdfFiles.add(pdf_archive);
		}
		else if ( su_id == null ) {
			logger.error( programName + ":setElgarPdfs: No su_id found for " + bookAuId );
			throw new Exception("No su_id found for book");
		}
		
	}

	
	private void setBenjaminsPdfs(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();
		
		int pdfFileCount = 0;
		
		try {
			pdfFileCount = TDMUtil.findPdfFileCountFromDB(bookAuId);
			if ( pdfFileCount == 1 ) {
				setSinglePdf(true);
			}
			
		} catch (Exception e3) {
			logger.error( programName + ":setBenjaminsPdfs: findPdfFileCountFromDB for " + bookAuId + " " + e3.getMessage());
			
		}
		
		//pdf file name is not stored in bits xml file. Get pdf file name from DB
		String pdf_archive = null;
		String su_id = null;
		
		if ( isSinglePdf() ) {  //benjamins only has 1 pdf file
			try {
				su_id = TDMUtil.findSuidByAuid( bookAuId);
			} catch (Exception e) {
				logger.error( programName + ":setBenjaminsPdfs: findSuidByAuid for " + providerID + " " + bookAuId + " " + e);
				e.printStackTrace();
				throw e;
			}		
		}
		else {
			logger.error( programName + ":setBenjaminsPdfs More than 1 pdf file used for Benjamins's book!");
			throw new Exception("More than 1 pdf file used for Benjamins book");
		}
		
		if ( su_id != null && !su_id.isEmpty()) {
			pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
			pdfFiles.add(pdf_archive);
		}
		else if ( su_id == null ) {
			logger.error( programName + ":setBenjaminsPdfs: No su_id found for " + bookAuId );
			throw new Exception("No su_id found for book");
		}
		
	}
	
	private void setSagePdfs(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();
		
		int pdfFileCount = 0;
		
		try {
			pdfFileCount = TDMUtil.findPdfFileCountFromDB(bookAuId);
			if ( pdfFileCount == 1 ) {
				setSinglePdf(true);
			}
			
		} catch (Exception e3) {
			logger.error( programName + ":setSagePdfs: findPdfFileCountFromDB for " + bookAuId + " " + e3.getMessage());
			
		}
		
		//pdf file name is not stored in bits xml file. Get pdf file name from DB
		String pdf_archive = null;
		String su_id = null;

		if ( isSinglePdf() ) {
			try {
				su_id = TDMUtil.findSuidByAuid( bookAuId);			
			} catch (Exception e) {
				logger.error( programName + ":setSagePdfs :findSuidByAuid " + providerID + " " + bookAuId + " " + e);
				throw e;
			}


		}
		else {	//there might be 2 pdfs
			logger.info( programName + ":processSageBookChapters More than 1 pdf file used for Sage's book!");
			try {
				su_id = TDMUtil.findSuidByAuidWithBiggerSize( bookAuId);			
			} catch (Exception e) {
				logger.error( programName + ":setSagePdfs :findSuidByAuid " + providerID + " " + bookAuId + " " + e);
				throw e;
			}
		}


		if ( su_id != null && !su_id.isEmpty()) {
			pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
			pdfFiles.add(pdf_archive);
		}
		else if ( su_id == null ) {
			logger.error( programName + ":setSagePdfs: No su_id found for " + bookAuId );
			throw new Exception("No su_id found for book");
		}
		
	}


	private void setEUPPdfs(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("//book-part[not(descendant::book-part)]", doc, XPathConstants.NODESET); 		//deepest level of book-part  //body/book-part/body/book-part
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error(programName + ":setEUPPdfs for " + providerID + " " + bookAuId + ":Cannot get doi " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/self-uri[@content-type='pdf']/@*[name()='xlink:href']");  //ark:/27927/pbd16v8g9dv
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":setEUPPdfs for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":setEUPPdfs :findSuidByFuid for " + providerID + " " + bookAuId + " " + e);
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":setEUPPdfs :findSuidByDOI for "  + providerID + " " + bookAuId + " " + e);
						continue;
					}
				}
			}
			
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfFiles.add(pdf_archive);
			}
			
		}
		
		if ( pdfFiles.isEmpty()) {
			throw new Exception("No PDF file found for EUP book " + bookAuId );
		}
	}

	private void setNOMOSPdfs(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();

		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book//book-part-meta[self-uri]", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpartmeta_elem = (Element) nodes.item(i);
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpartmeta_elem, "book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error(programName + ":setNOMOSPdfs for " + providerID + " " + bookAuId + ":Cannot get doi " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpartmeta_elem, "self-uri/@*[name()='xlink:href']");  //ark:/27927/pbqk9bf7p
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":setNOMOSPdfs for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":setNOMOSPdfs :findSuidByFuid for " + providerID + " " + bookAuId + " " + e);
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":setNOMOSPdfs :findSuidByDOI for " + providerID + " " + bookAuId + " " + e);
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfFiles.add(pdf_archive);
			}
		}
		
		if ( pdfFiles.isEmpty()) {
			throw new Exception("No PDF file found for NOMOS book " + bookAuId );
		}

	}

	private void setMUSEPdfs(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();

		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/body/book-part", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/alternate-form[@alternate-form-type='pdf']/@*[name()='xlink:href']");  
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":setMUSEPdfs for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			
			//no doi
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			//
				} catch (Exception e) {
					logger.error( programName + ":setMUSEPdfs :findSuidByFuid for " + providerID + " " + bookAuId + " " + e );
					continue;
				}
			}
			
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfFiles.add(pdf_archive);
			}
		}
		
		if ( pdfFiles.isEmpty()) {
			throw new Exception("No PDF file found for MUSE book " + bookAuId );
		}
		
	}

	private void setWileyPdfs(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();

		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book//book-part", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);

			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@pub-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error(programName + ":setWileyPdfs for " + providerID + " " + bookAuId + ":Cannot get doi " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/alternate-form[@alternate-form-type='pdf']/@*[name()='xlink:href']");  //ark:/27927/pbd17h34s0n
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":setWileyPdfs for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":setWileyPdfs :findSuidByFuid for " + providerID + " " + bookAuId + ":Cannot get su_id " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":setWileyPdfs :findSuidByDOI for " + providerID + " " + bookAuId + " " + e );
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfFiles.add(pdf_archive);
			}
			
		}
		
		if ( pdfFiles.isEmpty()) {
			throw new Exception("No PDF file found for Wiley book " + bookAuId );
		}
		
	}

	private void setRIAPdfs(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();

		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part[@book-part-type!='book-toc-page-order']", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
	
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error(programName + ":setRIAPdfs for " + providerID + " " + bookAuId + ":Cannot get doi " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/self-uri[@content-type='pdf']/@*[name()='xlink:href']");  //ark:/27927/pbqk9bf7p
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":setRIAPdfs for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":setRIAPdfs :findSuidByFuid for " + providerID + " " + bookAuId + " "  + e);
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":setRIAPdfs :findSuidByDOI for " + providerID + " " + bookAuId + " "  + e);
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfFiles.add(pdf_archive);
			}
		
		}
		
		if ( pdfFiles.isEmpty()) {
			throw new Exception("No PDF file found for RIA book " + bookAuId );
		}
		
	}

	private void setThiemePdfs(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();

		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("//book-part-meta[self-uri]", doc, XPathConstants.NODESET); 	//if has self-uri children, then we can get pdf file name	
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpartmeta_elem = (Element) nodes.item(i);
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpartmeta_elem, "book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error(programName + ":setThiemePdfs for " + providerID + " " + bookAuId + ":Cannot get doi " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpartmeta_elem, "self-uri/@*[name()='xlink:href']");  //ark:/27927/pbd1rz1z259
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":setThiemePdfs for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":setThiemePdfs :findSuidByFuid for " + providerID + " " + bookAuId + " " + e );
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":setThiemePdfs :findSuidByDOI for " + providerID + " " + bookAuId + " " + e );
						continue;
					}
				}
			}
			
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfFiles.add(pdf_archive);
			}
		}
		
		if ( pdfFiles.isEmpty()) {
			throw new Exception("No PDF file found for Thieme book " + bookAuId );
		}
		
		
		
	}

	private void setUCALPdfs(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();

		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);

			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error(programName + ":setUCALPdfs for " + providerID + " " + bookAuId + ":Cannot get doi " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/self-uri[@content-type='pdf']/@*[name()='xlink:href']");  //ark:/27927/pbd16v8g9dv
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":setUCALPdfs for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":setUCALPdfs :findSuidByDOI for " + providerID + " " + bookAuId + " " + e);
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":setUCALPdfs :findSuidByDOI for " + providerID + " " + bookAuId + " " + e);
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfFiles.add(pdf_archive);
			}
		}
		
		if ( pdfFiles.isEmpty()) {
			throw new Exception("No PDF file found for UCAL book " + bookAuId );
		}
		
		
	}

	private void setCAMBPdfs(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		pdfFiles = new ArrayList<>();

		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/body/book-part", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi' or @pub-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error(programName + ":setCAMBPdfs for " + providerID + " " + bookAuId + ":Cannot get doi " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/alternate-form[@alternate-form-type='pdf']/@*[name()='xlink:href']");  //ark:/27927/pbd681p79s
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":setCAMBPdfs for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":setCAMBPdfs :findSuidByFuid for " + providerID + " " + bookAuId + " " + e);
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":setCAMBPdfs :findSuidByDOI for " + providerID + " " + bookAuId + " " + e);
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfFiles.add(pdf_archive);
			}
		}
		
		if ( pdfFiles.isEmpty()) {
			throw new Exception("No PDF file found for Cambridge book " + bookAuId );
		}
	}




	/**
	 * See https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/book.html
	 * 		(collection-meta*, book-meta?, front-matter?, book-body?, book-back?)
	 * Process /book/front-matter, /book/book-body, /book/book-back
	 * Elgar: 		1 pdf for all chapters. Chapter metadata stored in /book/body/book-part
	 * Benjamins:	1 pdf for all chapters. No chapter metadata in /book. Use bookbooks in pdf file to get chapter info.
	 * EUP:			pdfs for chapters. Chapter metadata in /book/book-body
	 * NOMOS:		pdfs for chapters. Chapter metadata in /book/book-body/book-part
	 * MUSE:		pdfs for chapters. Chapter metadata in /book/body/book-part
	 * Wiley:		pdfs for chapters. Chapter metadata in /book/book-front, /book/body, /book/back
	 * RIA: 		pdfs for chapters. Chapter metadata in /book/book-body
	 * THIEME: 		pdfs for chapters. /book/book-body/book-part/body/book-part
	 * UCAL:		pdfs for chapters. Chapter metadata in /book/book-body/book-part
	 * CAMBRIDGE:	pdfs for chapters. Chapter metadata in /book/body/book-part
	 * PLUTOPRESS:
	 * BERGHAHN:
	 * SAGE: (2 content sets)
	 * MICHIGAN:
	 * EMERALD
	 * CSIRO:		1 pdf for all chapters. No chapter metadata in xml file. Use bookmarks in pdf file to get chapter info. Use ? in epub file for chapter info.
	 * 
	 * @param doc
	 * @throws Exception 
	 */
	private void populateBookChapters(Document doc) throws Exception {
		
		//process book chapters by publisher
		String providerID = getPublisherId();
		
		if ( providerID.equals("ELGAR")) {
			processElgarBookChapters(doc);
		}
		else if ( providerID.equals("BENJAMINS")) {
			processBenjaminsBookChapters(doc);
		}
		else if ( providerID.equals("EUP")) {
			processEUPBookChapters(doc);
		}
		else if ( providerID.equals("NOMOS")) {
			processNOMOSBookChapters(doc);
		}
		else if ( providerID.equals("MUSE")) {
			processMUSEBookChapters(doc);
		}
		else if ( providerID.equals("WILEY")) {
			processWileyBookChapters(doc);
		}
		else if ( providerID.equals("RIA")) {
			processRIABookChapters(doc);
		}
		else if ( providerID.equals("THIEME")) {
			processThiemeBookChapters(doc);
		}
		else if ( providerID.equals("UCAL")) {
			processUCALBookChapters(doc);
		}
		else if ( providerID.equals("CAMBRIDGE")) {
			processCAMBBookChapters(doc);
		}
		else if ( providerID.equals("PLUTOPRESS")) {
			processPlutoBookChapters(doc);
		}
		else if ( providerID.equals("BERGHAHN")) {
			processBerghahnBookChapters(doc);
		}
		else if ( providerID.equals("SAGE")) {
			processSageBookChapters(doc);
		}
		else if ( providerID.equals("MICHIGAN")) {
			processMichiganBookChapters(doc);
		}
		else if ( providerID.equals("EMERALD")) {
			processEmeraldBookChapters(doc);
		}
		else if ( providerID.equals("CSIRO")) {
			processCSIROBookChapters(doc);
		}
	}

	
	/**
	 * CSIRO： 1 pdf for all chapters. No chapter metadata. Use bookmark to retrieve chapter info.
	 * @param doc
	 * @throws Exception 
	 */
	private void processCSIROBookChapters(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		
		List<String> pdfs = getPdfFiles();
		String pdf_archive = pdfs.get(0);
		
		if ( pdf_archive == null ) {
			throw new Exception("No pdf file is found for CSIRO ebook " + bookAuId );
		}

		//Use  bookmarks to get chapter info
		List<String> chapterTitles = new ArrayList<>();
		String pdfFileWithPath = "input" + File.separatorChar + "ebook" + File.separator + getSubDir()  + File.separator 
									+ bookAuId.substring(bookAuId.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + pdf_archive;
		
		PDDocumentOutline bookmarkOutline = null;


		PDDocument pddoc = null;
		try {
			pddoc = PDDocument.load(new File(pdfFileWithPath)) ;

			int bookPageCount = pddoc.getNumberOfPages();
			if ( getPageCount() == 0 ) {
				setPageCount( bookPageCount );
			}

			bookmarkOutline =  pddoc.getDocumentCatalog().getDocumentOutline();

			PDOutlineItem current = bookmarkOutline.getFirstChild();

			int seq = 0;


			while (current != null) {
				seq++;

				PDOutlineItem next = current.getNextSibling();
				if ( next == current ) {
					logger.error( programName + ": Error pdf bookmark structure at node " + seq );
					throw new Exception("Error splitting pdf file by bookmark " + pdfFileWithPath );
				}
				int startPage = 0;				//starting from 1
				int lastPage = 0;
				int nextStartPage = 0;
				int chapterPageCount = 0;
				List<String> pageTextList = new ArrayList<>();
				String chapterTitle = current.getTitle();;

				if (current.getDestination() instanceof PDPageDestination) {
					PDPageDestination pd = (PDPageDestination) current.getDestination();
					startPage = pd.retrievePageNumber() + 1;
				}
				else if (current.getDestination() instanceof PDNamedDestination)
				{
					PDPageDestination pd = pddoc.getDocumentCatalog().findNamedDestinationPage((PDNamedDestination) current.getDestination());
					if (pd != null)
					{
						startPage = pd.retrievePageNumber() + 1;;
					}
				}
				if (current.getAction() instanceof PDActionGoTo) {
					PDActionGoTo gta = (PDActionGoTo) current.getAction();
					if (gta.getDestination() instanceof PDPageDestination) {
						PDPageDestination pd = (PDPageDestination) gta.getDestination();
						startPage = pd.retrievePageNumber() + 1;
					}
					else if (gta.getDestination() instanceof PDNamedDestination)
					{
						PDPageDestination pd = pddoc.getDocumentCatalog().findNamedDestinationPage((PDNamedDestination) gta.getDestination());
						if (pd != null)
						{
							startPage = pd.retrievePageNumber() + 1;
						}
					}
					else {
						continue;
					}
				}

				if ( next == null ) {		//last bookmark
					lastPage = bookPageCount;
					chapterPageCount = lastPage - startPage + 1;

					Splitter splitter = new Splitter();
					splitter.setStartPage(startPage);		//index starting from 1
					splitter.setEndPage(lastPage);
					try {
						// splitting the pages of a PDF document
						List<PDDocument> Pages = splitter.split(pddoc);

						// Creating an iterator
						Iterator<PDDocument> iterator = Pages.listIterator();

						// retrieve text from each page
						while (iterator.hasNext()) {
							PDDocument pd = iterator.next();
							String pageText = new PDFTextStripper().getText(pd);
							pageTextList.add(pageText);
						}

					} catch (IOException e) {
						logger.error( programName + ":processCSIROBookChapters Error retrieve page text from one chapter from pdf file " + e.getMessage() );
						e.printStackTrace();
					}


					chapterTitles.add(chapterTitle);


					break;

				}


				if ( next.getDestination() instanceof PDPageDestination) {
					PDPageDestination pd = (PDPageDestination) next.getDestination();
					nextStartPage = pd.retrievePageNumber() + 1;
				}
				else if (next.getDestination() instanceof PDNamedDestination)
				{
					PDPageDestination pd = pddoc.getDocumentCatalog().findNamedDestinationPage((PDNamedDestination) next.getDestination());
					if (pd != null)
					{
						nextStartPage = pd.retrievePageNumber() + 1;;
					}
				}

				else if (next.getAction() instanceof PDActionGoTo) {
					PDActionGoTo gta = (PDActionGoTo) next.getAction();
					if (gta.getDestination() instanceof PDPageDestination) {
						PDPageDestination pd = (PDPageDestination) gta.getDestination();
						nextStartPage = pd.retrievePageNumber() + 1;
					}
					else if (gta.getDestination() instanceof PDNamedDestination)
					{
						PDPageDestination pd = pddoc.getDocumentCatalog().findNamedDestinationPage((PDNamedDestination) gta.getDestination());
						if (pd != null)
						{
							nextStartPage = pd.retrievePageNumber() + 1;
						}
					}
					else {

						nextStartPage = startPage + 1;	//in case of error	
					}
				}

				if ( nextStartPage != 0 ) {	//this may not totally correct if chapter overlaps
					lastPage = nextStartPage -1 ;
					chapterPageCount = lastPage - startPage + 1;

					Splitter splitter = new Splitter();
					splitter.setStartPage(startPage);		//index starting from 1
					splitter.setEndPage(lastPage);
					try {
						// splitting the pages of a PDF document
						List<PDDocument> Pages = splitter.split(pddoc);

						// Creating an iterator
						Iterator<PDDocument> iterator = Pages.listIterator();

						// retrieve text from each page
						while (iterator.hasNext()) {
							PDDocument pd = iterator.next();
							String pageText = new PDFTextStripper().getText(pd);

							//remove end of line hyphens
							pageText = TDMUtil.dehypendateText(pageText);

							pageTextList.add(pageText);
						}

					} catch (IOException e) {
						logger.error( programName + ":processCSIROBookChapters Error retrieve page text from one chapter from pdf file " + e.getMessage() );
						e.printStackTrace();
						continue;
					}
				}

				current = next;

				chapterTitles.add(chapterTitle);

			}
			
			pddoc.close();

		}  //end of try PDDocument.load() 
		catch (InvalidPasswordException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
		finally {
			if( pddoc != null )
			{
				pddoc.close();
			}
		}
		
		setHasPartTitle( chapterTitles );
		
		if ( ! chapterTitles.isEmpty()) {
			logger.info( programName + ":processCSIROBookChapters : " +  chapterTitles.size() + " chapter titles have been retrieved for CSIRO book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processCSIROBookChapters : 0 chapter titles have been retrieved for CSIRO book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		
	}

	/*
	 * Elgar: 1 pdf for all chapters. Chapter metadata stored in /book/body//book-part
	 * Read chapter titles from doc and set hasPartTitle
	 * 	-> set hasPartTitle
	 * 
	 * Chapter meta in /book/body//book-part (different levels). 
	 * Even Chapter fulltext can be retrieved from the single pdf file using fpage and lpage number, do not create V2Chapter.
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\elgar\pbf34b30m\data\pbf36mm5t.xml ( pbf349str.pdf  )
	 */
	private void processElgarBookChapters(Document doc) throws XPathExpressionException {
		
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//all book-part under /book/body tree branch whose book-part-type!='PART'
		//NodeList nodes = (NodeList)xPath.evaluate("/book/body//book-part[@book-part-type!='PART']", doc, XPathConstants.NODESET); 
		//NodeList nodes = (NodeList)xPath.evaluate("/book/body//book-part[@book-part-type!='PART']", doc, XPathConstants.NODESET); 
		//Include PART?? TODO : test
		NodeList nodes = (NodeList)xPath.evaluate("/book/body//book-part", doc, XPathConstants.NODESET); 
		
		List<String> partTitles = new ArrayList<>();
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);

			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.info( programName + ":  Error getting one chapter title " + providerID + " " + bookAuId + " " + e2 );
			}

			partTitles.add(title);

		}

		setHasPartTitle( partTitles );
		
		if ( ! partTitles.isEmpty()) {
			logger.info( programName + ":processElgarBookChapters : " +  partTitles.size() + " chapter titles have been retrieved for Elgar book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processElgarBookChapters : 0 chapter titles have been retrieved for Elgar book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		
	}


	/*
	 * Benjamins: 1 pdf for all chapters. No chapter metadata in /book. Use bookmarks in pdf file to get chapter info.
	 * pdf file name is not stored in bits xml file
	 * 
	 * 	 * Do:
	 * 	-> set hasPartTitle
	 * 	-> Set pageStart, pageEnd, pagination
	 * 	-> set pageCount??
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\benjamins\pbd10gtj8zg\data\pbd10gtnj6x.xml (pbd10gtcsnv.pdf)
	 */
	private void processBenjaminsBookChapters(Document doc) throws Exception  {

		String providerID = getPublisherId();
		String bookAuId = getAuid();
		
		List<String> pdfs = getPdfFiles();
		String pdf_archive = pdfs.get(0);
		
		if ( pdf_archive == null ) {
			throw new Exception("No pdf file is found for Benjamins ebook " + bookAuId );
		}

		//Use  bookmarks to get chapter info
		List<String> chapterTitles = new ArrayList<>();
		String pdfFileWithPath = "input" + File.separatorChar + "ebook" + File.separator + getSubDir()  + File.separator 
									+ bookAuId.substring(bookAuId.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + pdf_archive;
		
		PDDocumentOutline bookmarkOutline = null;


		PDDocument pddoc = null;
		try {
			pddoc = PDDocument.load(new File(pdfFileWithPath)) ;

			int bookPageCount = pddoc.getNumberOfPages();
			if ( getPageCount() == 0 ) {
				setPageCount( bookPageCount );
			}

			bookmarkOutline =  pddoc.getDocumentCatalog().getDocumentOutline();

			PDOutlineItem current = bookmarkOutline.getFirstChild();

			int seq = 0;


			while (current != null) {
				seq++;

				PDOutlineItem next = current.getNextSibling();
				if ( next == current ) {
					logger.error( programName + ": Error pdf bookmark structure at node " + seq );
					throw new Exception("Error splitting pdf file by bookmark " + pdfFileWithPath );
				}
				int startPage = 0;				//starting from 1
				int lastPage = 0;
				int nextStartPage = 0;
				int chapterPageCount = 0;
				List<String> pageTextList = new ArrayList<>();
				String chapterTitle = current.getTitle();;

				if (current.getDestination() instanceof PDPageDestination) {
					PDPageDestination pd = (PDPageDestination) current.getDestination();
					startPage = pd.retrievePageNumber() + 1;
				}
				else if (current.getDestination() instanceof PDNamedDestination)
				{
					PDPageDestination pd = pddoc.getDocumentCatalog().findNamedDestinationPage((PDNamedDestination) current.getDestination());
					if (pd != null)
					{
						startPage = pd.retrievePageNumber() + 1;;
					}
				}
				if (current.getAction() instanceof PDActionGoTo) {
					PDActionGoTo gta = (PDActionGoTo) current.getAction();
					if (gta.getDestination() instanceof PDPageDestination) {
						PDPageDestination pd = (PDPageDestination) gta.getDestination();
						startPage = pd.retrievePageNumber() + 1;
					}
					else if (gta.getDestination() instanceof PDNamedDestination)
					{
						PDPageDestination pd = pddoc.getDocumentCatalog().findNamedDestinationPage((PDNamedDestination) gta.getDestination());
						if (pd != null)
						{
							startPage = pd.retrievePageNumber() + 1;
						}
					}
					else {
						continue;
					}
				}

				if ( next == null ) {		//last bookmark
					lastPage = bookPageCount;
					chapterPageCount = lastPage - startPage + 1;

					Splitter splitter = new Splitter();
					splitter.setStartPage(startPage);		//index starting from 1
					splitter.setEndPage(lastPage);
					try {
						// splitting the pages of a PDF document
						List<PDDocument> Pages = splitter.split(pddoc);

						// Creating an iterator
						Iterator<PDDocument> iterator = Pages.listIterator();

						// retrieve text from each page
						while (iterator.hasNext()) {
							PDDocument pd = iterator.next();
							String pageText = new PDFTextStripper().getText(pd);
							pageTextList.add(pageText);
						}

					} catch (IOException e) {
						logger.error( programName + ":processBenjaminsBookChapters Error retrieve page text from one chapter from pdf file " + e.getMessage() );
						e.printStackTrace();
					}


					chapterTitles.add(chapterTitle);


					break;

				}


				if ( next.getDestination() instanceof PDPageDestination) {
					PDPageDestination pd = (PDPageDestination) next.getDestination();
					nextStartPage = pd.retrievePageNumber() + 1;
				}
				else if (next.getDestination() instanceof PDNamedDestination)
				{
					PDPageDestination pd = pddoc.getDocumentCatalog().findNamedDestinationPage((PDNamedDestination) next.getDestination());
					if (pd != null)
					{
						nextStartPage = pd.retrievePageNumber() + 1;;
					}
				}

				else if (next.getAction() instanceof PDActionGoTo) {
					PDActionGoTo gta = (PDActionGoTo) next.getAction();
					if (gta.getDestination() instanceof PDPageDestination) {
						PDPageDestination pd = (PDPageDestination) gta.getDestination();
						nextStartPage = pd.retrievePageNumber() + 1;
					}
					else if (gta.getDestination() instanceof PDNamedDestination)
					{
						PDPageDestination pd = pddoc.getDocumentCatalog().findNamedDestinationPage((PDNamedDestination) gta.getDestination());
						if (pd != null)
						{
							nextStartPage = pd.retrievePageNumber() + 1;
						}
					}
					else {

						nextStartPage = startPage + 1;	//in case of error	
					}
				}

				if ( nextStartPage != 0 ) {	//this may not totally correct if chapter overlaps
					lastPage = nextStartPage -1 ;
					chapterPageCount = lastPage - startPage + 1;

					Splitter splitter = new Splitter();
					splitter.setStartPage(startPage);		//index starting from 1
					splitter.setEndPage(lastPage);
					try {
						// splitting the pages of a PDF document
						List<PDDocument> Pages = splitter.split(pddoc);

						// Creating an iterator
						Iterator<PDDocument> iterator = Pages.listIterator();

						// retrieve text from each page
						while (iterator.hasNext()) {
							PDDocument pd = iterator.next();
							String pageText = new PDFTextStripper().getText(pd);

							//remove end of line hyphens
							pageText = TDMUtil.dehypendateText(pageText);

							pageTextList.add(pageText);
						}

					} catch (IOException e) {
						logger.error( programName + ":processBenjaminsBookChapters Error retrieve page text from one chapter from pdf file " + e.getMessage() );
						e.printStackTrace();
						continue;
					}
				}

				current = next;

				chapterTitles.add(chapterTitle);

			}
			
			pddoc.close();

		}  //end of try PDDocument.load() 
		catch (InvalidPasswordException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
		finally {
			if( pddoc != null )
			{
				pddoc.close();
			}
		}
		
		setHasPartTitle( chapterTitles );
		
		
		if ( ! chapterTitles.isEmpty()) {
			logger.info( programName + ":processBenjaminsBookChapters : " +  chapterTitles.size() + " chapter titles have been retrieved for Benjamins book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processBenjaminsBookChapters : 0 chapter titles have been retrieved for Benjamins book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		
	}


	/*
	 * EUP: pdfs for chapters. Chapter metadata in /book/book-body
	 * 	Do:
	 * 	-> set hasPartTitle
	 * 	-> Set pdfFiles
	 *  -> set chapters
	 *  
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\eup\pbd16v8j3gt\data\pbd16v8gvq3.xml (13 pdfs)
	 * Tested
	 */
	private void processEUPBookChapters(Document doc) throws Exception  {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("//book-part[not(descendant::book-part)]", doc, XPathConstants.NODESET); 		//deepest level of book-part  //body/book-part/body/book-part

		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + "processEUPBookChapters: : Error getting title for book chapter" + providerID + " "  + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/subtitle");
			} catch (XPathExpressionException e2) {
				
			}
			String seqStr = bookpart_elem.getAttribute("seq");
			int seq = 0;
			if ( seqStr != null && !seqStr.isEmpty()) {
				seq = Integer.parseInt(seqStr);
			}
			else {
				seq = i+1;
			}
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + "processEUPBookChapters: : Error getting DOI for book chapter" + providerID + " " + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/self-uri[@content-type='pdf']/@*[name()='xlink:href']");  //ark:/27927/pbd16v8g9dv
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processEUPBookChapters for " + bookAuId + ":Cannot get fu_id " + e1);
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":processEUPBookChapters :findSuidByFuid for " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":processEUPBookChapters :findSuidByDOI for " + bookAuId + " " + e);
						continue;
					}
				}
			}

			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";	
				pdfs.add(pdf_archive);
			}

			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processEUPBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processEUPBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			
			//EUP no chapter page count
			
			String chapter_abstract = null;
			try {
				chapter_abstract = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/abstract");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processEUPBookChapters :Error getting chapter abstract for book chapter " + bookAuId + " " + e);
			}
			
			List<String> lans = new ArrayList<>();
			String lang = bookpart_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpart_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processEUPBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);

		    //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setAbstractStr(chapter_abstract);
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		setHasPartTitle(chapterTitles);
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for EUP book " + bookAuId );
		}
		
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processEUPBookChapters : " +  chapters.size() + " chapter titles have been retrieved for EUP book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processEUPBookChapters : 0 chapter have been retrieved for EUP book " + bookAuId + " from " + getBitsXmlFileName() );
		}
	}

	
	/**
	 * Get book's ISBN ids
	 * @return
	 */
	private List<Identifier> getBookISBNIds() {
		List<Identifier> bookIds = getIdentifiers();
		List<Identifier> selectedIds = new ArrayList<>();
		for(Identifier id: bookIds) {
			if ( id.getName().equals("isbn") ) {
				selectedIds.add(id);
			}
		}
		
		return selectedIds;
	}

	private List<String> getBookChapterCreators(Element bookpart_elem) throws XPathExpressionException {
		List<String> creators = new ArrayList<>();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		XPathExpression expr = xPath.compile("./book-part-meta/contrib-group/contrib");
		
        NodeList resultNodes = (NodeList) expr.evaluate(bookpart_elem, XPathConstants.NODESET);
        
        for (int i = 0; i < resultNodes.getLength(); ++i) {
        	Element e = (Element) resultNodes.item(i);
		    Person creator = parseAContribNode( e );
		    if ( creator != null ) {
		    	creators.add(creator.getFullName());
		    }
        }
        
		if ( creators.isEmpty()) {
			creators = null;
		}
		
		return creators;
	}

	
	/**
	 * 	pdfs for chapters. Chapter metadata in /book//book-part-meta
	 *
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\nomos\phx73m9gwmg\data\phx73mtt98q.xml  (104 pdfs)
	 * @throws Exception 
	 */
	private void processNOMOSBookChapters(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book//book-part-meta[self-uri]", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpartmeta_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpartmeta_elem, "title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + "processNOMOSBookChapters: : Error getting title for book chapter" + providerID + " " + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpartmeta_elem, "title-group/subtitle");
			} catch (XPathExpressionException e2) {

			}

			int seq = i+1;
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpartmeta_elem, "book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + "processNOMOSBookChapters: : Error getting DOI for NOMOS book chapter" + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpartmeta_elem, "self-uri/@*[name()='xlink:href']");  //ark:/27927/pbqk9bf7p
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processNOMOSBookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":processNOMOSBookChapters :findSuidByFuid for " + providerID + " " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":processNOMOSBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}
			
			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpartmeta_elem, "fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processNOMOSBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpartmeta_elem, "lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processNOMOSBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			//no page count for NOMOS chapters
			

			List<String> lans = new ArrayList<>();
			String lang = bookpartmeta_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpartmeta_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processNOMOSBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);
		    
		  //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for NOMOS book " + bookAuId );
		}
		
		setHasPartTitle(chapterTitles);
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processNOMOSBookChapters : " +  chapters.size() + " chapter titles have been retrieved for NOMOS book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processNOMOSBookChapters : 0 chapter have been retrieved for NOMOS book " + bookAuId + " from " + getBitsXmlFileName() );
		}
	}

	/*
	 * Muse: pdfs for chapters. Chapter metadata in /book/body/book-part
	 * Some MUSE books don't have body in bitsXML file. ie. ark:/27927/phz2cf8m90s
	 * 
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\muse\pbd16g5v916\data\pbd16g5w4fq.xml  ( 35 pdfs ) 
	 */
	private void processMUSEBookChapters(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/body/book-part", doc, XPathConstants.NODESET); 
		
		if ( nodes == null || nodes.getLength() == 0 ) {	//no body part in xml file
			processMUSEBookChapterFromDB();
			return;
		}
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + "processMUSEBookChapters: : Error getting title for book chapter" + providerID + " " + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/subtitle");
			} catch (XPathExpressionException e2) {

			}
			
			String seqStr = bookpart_elem.getAttribute("book-part-number");		
			int seq = 0;
			if ( seqStr != null && !seqStr.isEmpty()) {
				seq = Integer.parseInt(seqStr);
			}
			else {
				seq = i+1;
			}
			
			String doi = null;  //no doi
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/alternate-form[@alternate-form-type='pdf' or @alternate-form-type='chapter-pdf' ]/@*[name()='xlink:href']");  
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processMUSEBookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			//
				} catch (Exception e) {
					logger.error( programName + ":processMUSEBookChapters :findSuidByFuid for " + providerID + " " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}
			
			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processMUSEBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processMUSEBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			//no MUSE chapter page count
			

			List<String> lans = new ArrayList<>();
			String lang = bookpart_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpart_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processMUSEBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);
		    chapter.setDoi(doi);		//null
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for MUSE book " + bookAuId );
		}
		
		setHasPartTitle(chapterTitles);
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processMUSEBookChapters : " +  chapters.size() + " chapter titles have been retrieved for MUSE book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processMUSEBookChapters : 0 chapter have been retrieved for MUSE book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		
	}


	private void processMUSEBookChapterFromDB() throws Exception {
		
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		List<String> lans = getLanguage();
		List<String> creators = getCreator();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		String su_query = "select * from a_su where a_au_id='" + bookAuId + "' and pmd_status='Active' and pmd_mime_type='application/pdf'";

		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(su_query);

			int seq = 1;
			while ( rs.next() ) {
				String su_id = rs.getString("pmd_object_id");
				String pdf_archive = rs.getString("pmd_archive_file_name");
				pdfs.add(pdf_archive);
				
				String title = "[Chapter]";
				chapterTitles.add(title);
				
				V2Chapter chapter = new V2Chapter(su_id);
			    chapter.setParentBook(this);
			    chapter.setBookAuid(bookAuId);
			    chapter.setInputDir( getInputDir());
			    chapter.setOutputDir(getOutputDir());
			    chapter.setContentSetName(bookCSName);
			    chapter.setSubDir(getSubDir());
			    chapter.setTitle(title);
			    chapter.setSequence(seq++);
			    chapter.setLanguage(lans);
			    chapter.setCreator(creators);

			    
			    List<String> outputformat = new ArrayList<>();
			    outputformat.add("unigram");
			    
			    if ( isFullTextAvailable() ) {
			    	chapter.setFullTextAvailable(true);
			    	outputformat.add("fulltext");
			    }
			    else {
			    	chapter.setFullTextAvailable(false);
			    }
			    chapter.setOutputFormat(outputformat);
			    //chapter.setIsPartOf(bookAuId);
			    String bookTitle = getTitle();
			    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
			    chapter.setPublicationYear(getPublicationYear());
			    chapter.setPublisher(getPublisher());
			    chapter.setDatePublished(getDatePublished());
			    chapter.setPublisherId(getPublisherId());
			    chapter.setPdfFile(pdf_archive);
			    
			    chapters.add(chapter);
			}

			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
	
		setHasPartTitle(chapterTitles);
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processMUSEBookChapterFromDB : " +  chapters.size() + " chapter titles have been retrieved from DB for MUSE book " + bookAuId  );
		}
		else {
			logger.info( programName + ":processMUSEBookChapterFromDB : 0 chapter have been retrieved from DB for MUSE book " + bookAuId  );
		}
		
	}

	/**
	 * No agreement for Wiley ebooks. 12/9/2021
	 * @param doc
	 * @throws Exception 
	 */
	private void processWileyBookChapters(Document doc) throws Exception {
		
/*		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/body/book-part", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + ":processWileyBookChapters : Error getting title for book chapter" + providerID + " "  + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/subtitle");
			} catch (XPathExpressionException e2) {
				
			}
			String seqStr = bookpart_elem.getAttribute("book-part-number");
			int seq = 0;
			if ( seqStr != null && !seqStr.isEmpty()) {
				seq = Integer.parseInt(seqStr);
			}
			else {
				seq = i+1;
			}
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processWileyBookChapters : Error getting DOI for book chapter" + providerID + " " + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/alternate-form[@alternate-form-type='pdf']/@*[name()='xlink:href']");  
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processUCALBookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":processWileyBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	
					} catch (Exception e) {
						logger.error( programName + ":processWileyBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}
			

			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processWileyBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processWileyBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			
			String chapter_abstract = null;
			try {
				chapter_abstract = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/abstract");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processWileyBookChapters :Error getting chapter abstract for book chapter " + bookAuId + " " + e);
			}
			
			List<String> lans = new ArrayList<>();
			String lang = bookpart_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpart_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processWileyBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    
			V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);

		    //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setAbstractStr(chapter_abstract);
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		setHasPartTitle(chapterTitles);
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for WILEY book " + bookAuId );
		}
		
		setPdfFiles(pdfs);
		setChapters(chapters);

		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processWileyBookChapters : " +  chapters.size() + " chapter titles have been retrieved for UCAL book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processWileyBookChapters : 0 chapter have been retrieved for UCAL book " + bookAuId + " from " + getBitsXmlFileName() );
		}
	
		*/
	}


	/*
	 * Michigan: pdfs for chapters. Chapter metadata in /book/book-body
	 * Do:
	 * 	-> set hasPartTitle
	 * 	-> Set pdfFiles
	 *  -> set chapters
	 *  
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\ria\phx7cwb2b51\data\phx7cwb6248.xml (18 pdfs)
	 */
	private void processMichiganBookChapters(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part[@book-part-type!='book-toc-page-order']", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + ": : Error getting title for book chapter" + providerID + " " + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/subtitle");
			} catch (XPathExpressionException e2) {
				
			}
			String seqStr = bookpart_elem.getAttribute("seq");
			int seq = 0;
			if ( seqStr != null && !seqStr.isEmpty()) {
				seq = Integer.parseInt(seqStr);
			}
			else {
				seq = i+1;
			}
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + "processMichiganBookChapters: : Error getting DOI for book chapter" + providerID + " " + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/self-uri[@content-type='pdf' or @content-type='chapter-pdf']/@*[name()='xlink:href']");  //ark:/27927/pbqk9bf7p
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processMichiganBookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":processMichiganBookChapters :findSuidByFuid for " + providerID + " " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":processMichiganBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}
			
			if ( su_id == null  ) {
				logger.error( programName + ": processMichiganBookChapters Null fu_id or su_id for book " + bookAuId + " in bits xml file " + getBitsXmlFileName() + " for chapter #" + seq );
				throw new Exception("Null su_id for book AU ");
			}
			

			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processMichiganBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processMichiganBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			String chapter_abstract = null;
			try {
				chapter_abstract = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/abstract");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processMichiganBookChapters :Error getting chapter abstract for book chapter " + bookAuId + " " + e);
			}
			
			
			List<String> lans = new ArrayList<>();
			String lang = bookpart_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpart_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processMichiganBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setAbstractStr(chapter_abstract);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);
		    //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		setHasPartTitle(chapterTitles);
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for Michigan book " + bookAuId );
		}
		
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processMichiganBookChapters : " +  chapters.size() + " chapter titles have been retrieved for Michigan book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processMichiganBookChapters : 0 chapter have been retrieved for Michigan book " + bookAuId + " from " + getBitsXmlFileName() );
		}
	
	}
	
	
	/*
	 * Emerald: pdfs for chapters. Chapter metadata in /book/book-body
	 * Do:
	 * 	-> set hasPartTitle
	 * 	-> Set pdfFiles
	 *  -> set chapters
	 *  
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\emerald\pbf100cgt1\data\pbf104m37q.xml (21 pdfs)
	 */
	private void processEmeraldBookChapters(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + ": : Error getting title for book chapter" + providerID + " " + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/subtitle");
			} catch (XPathExpressionException e2) {
				
			}
			String seqStr = bookpart_elem.getAttribute("seq");
			int seq = 0;
			if ( seqStr != null && !seqStr.isEmpty()) {
				seq = Integer.parseInt(seqStr);
			}
			else {
				seq = i+1;
			}
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + "processEmeraldBookChapters: : Error getting DOI for book chapter" + providerID + " " + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/self-uri[@content-type='pdf']/@*[name()='xlink:href']");  //ark:/27927/pbf100chd9
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processEmeraldBookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":processEmeraldBookChapters :findSuidByFuid for " + providerID + " " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//
					} catch (Exception e) {
						logger.error( programName + ":processEmeraldBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}
			

			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processEmeraldBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processEmeraldBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			String chapter_abstract = null;
			try {
				chapter_abstract = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/abstract");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processEmeraldBookChapters :Error getting chapter abstract for book chapter " + bookAuId + " " + e);
			}
			
			
			List<String> lans = new ArrayList<>();
			String lang = bookpart_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpart_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processEmeraldBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setAbstractStr(chapter_abstract);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);
		    //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		setHasPartTitle(chapterTitles);
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for Emerald book " + bookAuId );
		}
		
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processEmeraldBookChapters : " +  chapters.size() + " chapter titles have been retrieved for Emerald book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processEmeraldBookChapters : 0 chapter have been retrieved for Emerald book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		
	}


	/*
	 * Sage KNOWLEDGE: 			1 pdf for all chapters. Chapter metadata stored in /book/front-matter|book-body|book-back//book-part
	 * Read chapter titles from doc and set hasPartTitle
	 * 	-> set hasPartTitle
	 * 
	 * Sage RESEARCH METHODS: 	1 pdf for all chapters. Chapter metadata stored in /book/front-matter|book-body|book-back//book-part
	 * Read chapter titles from doc and set hasPartTitle
	 * 	-> set hasPartTitle
	 * 
	 * Chapter meta in /book//book-part (different levels). 
	 * Even Chapter fulltext can be retrieved from the single pdf file using fpage and lpage number, do not create V2Chapter.
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\sage\phz953gq3pv\data\phz953gr89v.xml  SAGE KNOWLEDGE ( phz953gpvgf.pdf  )  phz958q91zn.xml (SAGE RESEARCH)
	 */
	private void processSageBookChapters(Document doc) throws XPathExpressionException {
		
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		
		XPath xPath = XPathFactory.newInstance().newXPath();

		NodeList nodes = (NodeList)xPath.evaluate("/book//book-part", doc, XPathConstants.NODESET); 
		
		List<String> partTitles = new ArrayList<>();
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);

			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.info( programName + ":  Error getting one chapter title " + providerID + " " + bookAuId + " " + e2 );
			}

			partTitles.add(title);

		}

		setHasPartTitle( partTitles );
		
		
		if ( ! partTitles.isEmpty()) {
			logger.info( programName + ":processSageBookChapters : " +  partTitles.size() + " chapter titles have been retrieved for SAGE book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processSageBookChapters : 0 chapter have been retrieved for SAGE book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		
	}


	private void processBerghahnBookChapters(Document doc) {
		// TODO Auto-generated method stub
		
	}

	private void processPlutoBookChapters(Document doc) {
		// TODO Auto-generated method stub
		
	}

	/*
	 * RIA: pdfs for chapters. Chapter metadata in /book/book-body
	 * Do:
	 * 	-> set hasPartTitle
	 * 	-> Set pdfFiles
	 *  -> set chapters
	 *  
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\ria\phx7cwb2b51\data\phx7cwb6248.xml (18 pdfs)
	 */
	private void processRIABookChapters(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part[@book-part-type!='book-toc-page-order']", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + "processRIABookChapters: : Error getting title for book chapter" + providerID + " " + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/subtitle");
			} catch (XPathExpressionException e2) {
				
			}
			String seqStr = bookpart_elem.getAttribute("seq");
			int seq = 0;
			if ( seqStr != null && !seqStr.isEmpty()) {
				seq = Integer.parseInt(seqStr);
			}
			else {
				seq = i+1;
			}
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + "processRIABookChapters: : Error getting DOI for book chapter" + providerID + " " + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/self-uri[@content-type='pdf' or @content-type='chapter-pdf']/@*[name()='xlink:href']");  //ark:/27927/pbqk9bf7p
				
				
				
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processRIABookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				
				if ( fu_id.startsWith("localfile")) {    //localfile:j.ctt1g69w6r.1.pdf  in input/ebook/newcontent_202110/ria/phzfq069rtq/data/phzfq072qxx.xml
					su_id = TDMUtil.findSuidByFileName( fu_id, bookAuId);
				}
				else {
					try {
						su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
					} catch (Exception e) {
						logger.error( programName + ":processRIABookChapters :findSuidByFuid for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":processRIABookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}
			

			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processRIABookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processRIABookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			List<String> lans = new ArrayList<>();
			String lang = bookpart_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpart_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processRIABookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);
		    //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		setHasPartTitle(chapterTitles);
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for RIA book " + bookAuId );
		}
		
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processRIABookChapters : " +  chapters.size() + " chapter titles have been retrieved for RIA book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processRIABookChapters : 0 chapter have been retrieved for RIA book " + bookAuId + " from " + getBitsXmlFileName() );
		}
	
	}


	/*
	 * THIEME: 	pdfs for chapters. xml file has book-part title name and doi, no page/count info.
	 * 
	 *  Sample: C:\workspace_neon\TDM-pilot\input\ebook\thieme\pbd1rvvs5tf\data\pbd1rvvzxdm.xml (22 pdfs)
	 */
	private void processThiemeBookChapters(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		int seq = 1;
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("//book-part-meta[self-uri]", doc, XPathConstants.NODESET); 	//if has self-uri children, then we can get pdf file name	
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpartmeta_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpartmeta_elem, "title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + ":processThiemeBookChapters : Error getting title for book chapter" + providerID + " " + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpartmeta_elem, "title-group/subtitle");
			} catch (XPathExpressionException e2) {
				
			}
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpartmeta_elem, "book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processThiemeBookChapters : Error getting DOI for book chapter" + providerID + " " + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpartmeta_elem, "self-uri/@*[name()='xlink:href']");  //ark:/27927/pbd1rz1z259
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processThiemeBookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":processThiemeBookChapters :findSuidByFuid for " + providerID + " " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	//doi 10.3366/j.ctt9qdrrf.1
					} catch (Exception e) {
						logger.error( programName + ":processThiemeBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}
			else {
				continue;		//if there is no pdf file for the chapter, then we skip this chapter
			}
			
			//thieme doesn't have fpage, lpage
			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpartmeta_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processThiemeBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpartmeta_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processThiemeBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			List<String> lans = new ArrayList<>();
			String lang = bookpartmeta_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpartmeta_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processThiemeBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    
		    V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);
		    //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		setHasPartTitle(chapterTitles);
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for Thieme book " + bookAuId );
		}
		
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processThiemeBookChapters : " +  chapters.size() + " chapter titles have been retrieved for Thieme book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processThiemeBookChapters : 0 chapter have been retrieved for Thieme book " + bookAuId + " from " + getBitsXmlFileName() );
		}

		
		
	}



	/*
	 * UCAL: pdfs for chapters. Chapter metadata in /book/book-body/book-part
	 * 
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\ ucal\pbd16v86c1z\data\/pbd16v89b76.xml ( 13 pdfs )
	 */
	private void processUCALBookChapters(Document doc) throws Exception {
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + ":processUCALBookChapters : Error getting title for book chapter" + providerID + " "  + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/subtitle");
			} catch (XPathExpressionException e2) {
				
			}
			String seqStr = bookpart_elem.getAttribute("seq");
			int seq = 0;
			if ( seqStr != null && !seqStr.isEmpty()) {
				seq = Integer.parseInt(seqStr);
			}
			else {
				seq = i+1;
			}
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processUCALBookChapters : Error getting DOI for book chapter" + providerID + " " + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/self-uri[@content-type='pdf' or @content-type='chapter-pdf']/@*[name()='xlink:href']");  
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processUCALBookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":processUCALBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	
					} catch (Exception e) {
						logger.error( programName + ":processUCALBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}
			

			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processUCALBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processUCALBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			
			String chapter_abstract = null;
			try {
				chapter_abstract = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/abstract");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processUCALBookChapters :Error getting chapter abstract for book chapter " + bookAuId + " " + e);
			}
			
			List<String> lans = new ArrayList<>();
			String lang = bookpart_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpart_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processUCALBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
		    
			V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);

		    //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setAbstractStr(chapter_abstract);
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		setHasPartTitle(chapterTitles);
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for UCAL book " + bookAuId );
		}
		
		setPdfFiles(pdfs);
		setChapters(chapters);

		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processUCALBookChapters : " +  chapters.size() + " chapter titles have been retrieved for UCAL book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processUCALBookChapters : 0 chapter have been retrieved for UCAL book " + bookAuId + " from " + getBitsXmlFileName() );
		}

		
	}


	/*
	 * CAMBRIDGE: pdfs for chapters. Chapter metadata in /book/body/book-part
	 * 
	 * Sample: C:\workspace_neon\TDM-pilot\input\ebook\camb\pbd681c2p1\data\pbd681gzc8.xml ( 12 pdfs )
	 */
	private void processCAMBBookChapters(Document doc) throws Exception {
		
		String providerID = getPublisherId();
		String bookAuId = getAuid();
		String bookCSName = getContentSetName();
		
		List<String> pdfs = new ArrayList<>();
		List<String> chapterTitles = new ArrayList<>();
		List<V2Chapter> chapters = new ArrayList<>();
		
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		//NodeList nodes = (NodeList)xPath.evaluate("/book/book-body/book-part", doc, XPathConstants.NODESET); 
		NodeList nodes = (NodeList)xPath.evaluate("/book/body/book-part", doc, XPathConstants.NODESET); 		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element bookpart_elem = (Element) nodes.item(i);
			
			String title = null;
			try {
				title = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/title");
			} catch (XPathExpressionException e2) {
				logger.error( programName + ": : Error getting title for book chapter" + providerID + " "  + bookAuId + " " + e2);
				continue;
			}
			chapterTitles.add(title);
			
			String subTitle = null;
			try {
				subTitle = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/title-group/subtitle");
			} catch (XPathExpressionException e2) {
				
			}
			String seqStr = bookpart_elem.getAttribute("seq");
			int seq = 0;
			if ( seqStr != null && !seqStr.isEmpty()) {
				seq = Integer.parseInt(seqStr);
			}
			else {
				seq = i+1;
			}
			
			String doi = null;
			try {
				doi = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/book-part-id[@book-part-id-type='doi' or @pub-id-type='doi']");
			} catch (XPathExpressionException e) {
				logger.error( programName + ": : Error getting DOI for Cambridge book chapter" + bookAuId + " " + e);
			}
			
			String pdf_archive = null;
			String fu_id = null; String su_id = null;
			try {
				fu_id = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/alternate-form[@alternate-form-type='pdf']/@*[name()='xlink:href']");  
			} catch (XPathExpressionException e1) {
				logger.error( programName + ":processCAMBBookChapters for " + providerID + " " + bookAuId + ":Cannot get fu_id " + e1.getMessage());
				continue;
				
			}
			if ( fu_id != null ) {
				try {
					su_id = TDMUtil.findSuidByFuid( fu_id, bookAuId);			
				} catch (Exception e) {
					logger.error( programName + ":processCAMBBookChapters :findSuidByFuid for " + providerID + " " + bookAuId + " " + e.getMessage());
					continue;
				}
			}
			else {
				if ( doi != null && ! doi.isEmpty()) {
					try {
						su_id = TDMUtil.findSuidByDOI( doi.substring(doi.lastIndexOf("/")), bookAuId);	
					} catch (Exception e) {
						logger.error( programName + ":processCAMBBookChapters :findSuidByDOI for " + providerID + " " + bookAuId + " " + e.getMessage());
						continue;
					}
				}
			}
			if ( su_id != null && !su_id.isEmpty()) {
				pdf_archive = su_id.replace("ark:/27927/", "") + ".pdf";		
				pdfs.add(pdf_archive);
			}

			String fpage = null;
			try {
				fpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/fpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processCAMBBookChapters :Error getting fpage for book chapter " + bookAuId + " " + e);
			}
			String lpage = null;
			try {
				lpage = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/lpage");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processCAMBBookChapters :Error getting lpage for book chapter " + bookAuId + " " + e);
			}
			
			String pageRange = null;
			if ( fpage!= null && lpage != null ) {
				pageRange = fpage + "-" + lpage;
			}
			else if ( fpage != null ) {
				pageRange = fpage + "-";
			}
			
			String chapter_abstract = null;
			try {
				chapter_abstract = getSubtreeNodeValueByXPath( bookpart_elem, "book-part-meta/abstract");
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processCAMBBookChapters :Error getting chapter abstract for book chapter " + bookAuId + " " + e);
			}
			
						
			List<String> lans = new ArrayList<>();
			String lang = bookpart_elem.getAttribute("xml:lang");
			if ( lang == null || lang.isEmpty()) {
				//use book's language
				lans = getLanguage();
			}
			else {
				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				lans.add(lang3letter);
			}
			
			List<String> creators = null;
			try {
				creators = getBookChapterCreators(bookpart_elem);
			} catch (XPathExpressionException e) {
				logger.error( programName + ":processCAMBBookChapters :Error getting chapter creators for book chapter " + bookAuId + " " + e);
			}
			if ( creators == null || creators.isEmpty()) {
				//Use book's creator
				creators = getCreator();
			}
		    
			V2Chapter chapter = new V2Chapter(su_id);
		    chapter.setParentBook(this);
		    chapter.setBookAuid(bookAuId);
		    chapter.setInputDir( getInputDir());
		    chapter.setOutputDir(getOutputDir());
		    chapter.setContentSetName(bookCSName);
		    chapter.setSubDir(getSubDir());
		    chapter.setTitle(title);
		    chapter.setSubTitle(subTitle);
		    chapter.setSequence(seq);
		    chapter.setPageStart(fpage);
		    chapter.setPageEnd(lpage);
		    chapter.setPagination(pageRange);
		    chapter.setLanguage(lans);
		    chapter.setCreator(creators);

		    //copy book's isbns
		    List<Identifier> ids = getBookISBNIds();
		    if ( doi != null && !doi.isEmpty()) {	//if chapter has doi
		    	Identifier id;
		    	try {
		    		id = new Identifier("doi", doi);
		    		ids.add(id);
		    		String url = "https://doi.org/" + doi;
		    		chapter.setUrl(url);
		    	} catch (Exception e) {

		    	}
		    }
		    else if ( getDoi() != null ) {	//else use book's doi
		    	doi = getDoi();
		    }
		    chapter.setDoi(doi);
		    chapter.setIdentifiers(ids);
		    
		    List<String> outputformat = new ArrayList<>();
		    outputformat.add("unigram");
		    
		    if ( isFullTextAvailable() ) {
		    	chapter.setFullTextAvailable(true);
		    	outputformat.add("fulltext");
		    }
		    else {
		    	chapter.setFullTextAvailable(false);
		    }
		    chapter.setOutputFormat(outputformat);
		    //chapter.setIsPartOf(bookAuId);
		    String bookTitle = getTitle();
		    chapter.setIsPartOf(bookTitle);			//use book title instead of book id
		    chapter.setPublicationYear(getPublicationYear());
		    chapter.setAbstractStr(chapter_abstract);
		    chapter.setPublisher(getPublisher());
		    chapter.setDatePublished(getDatePublished());
		    chapter.setPublisherId(getPublisherId());
		    chapter.setPdfFile(pdf_archive);
		    
		    chapters.add(chapter);
		    
		}
		
		setHasPartTitle(chapterTitles);
		
		if ( pdfs.isEmpty()) {
			throw new Exception("No PDF file found for CAMBRIDGE book " + bookAuId );
		}
		
		setPdfFiles(pdfs);
		setChapters(chapters);
		
		if ( ! chapters.isEmpty()) {
			logger.info( programName + ":processCAMBBookChapters : " +  chapters.size() + " chapter titles have been retrieved for Cambridge book " + bookAuId + " from " + getBitsXmlFileName() );
		}
		else {
			logger.info( programName + ":processCAMBBookChapters : 0 chapter have been retrieved for Cambridge book " + bookAuId + " from " + getBitsXmlFileName() );
		}

		
	}
	
	
	
	/**
	 * Take epub dates, if not avaialbe, take ppub dates.
	 * Set datePublished, publicationYear
	 * Most:
	 *    <pub-date pub-type="epub">
     *		 <day>15</day>
     *		 <month>12</month>
     *		 <year>2014</year>
     * 		<string-date>20141215</string-date>
     *	  </pub-date>
	 * Cambridge: 
	 *       <pub-date pub-type="ppub">
     *    		<year>1983</year>
     *    		<string-date>1983</string-date>
     * 		</pub-date>
     * 		<pub-date pub-type="epub">
     *    		<year>2009</year>
     *    		<string-date>2009</string-date>
     * 		</pub-date>
	 * @param doc
	 * @throws XPathExpressionException 
	 * @throws IllegalArgumentException 
	 */
	private void setBookPubDate(Document doc) throws XPathExpressionException , IllegalArgumentException  {
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-meta/pub-date[@pub-type='epub']", doc, XPathConstants.NODESET);  
		
		if ( nodes != null && nodes.getLength() > 0 ) {
			Element pubdate_elem = (Element) nodes.item(0);
			String yearStr = getSubtreeElementValueByTagName(pubdate_elem, "year");
			String dayStr = getSubtreeElementValueByTagName(pubdate_elem, "day");			//01, 14
			String monthStr = getSubtreeElementValueByTagName(pubdate_elem, "month");		//July?, or 07
			
			int yearDigit = Integer.parseInt(yearStr);
			setPublicationYear( yearDigit );
			
			//YYYY-MM-DD
			if ( monthStr == null ) {
				monthStr = "01";
			}
			if ( dayStr == null ) {
				dayStr = "01";
			}
			String pub_date_str = yearStr + "-" + monthStr + "-" + dayStr;
			setDatePublished( pub_date_str );
			
		}
		else {
			nodes = (NodeList)xPath.evaluate("/book/book-meta/pub-date[@pub-type='ppub']", doc, XPathConstants.NODESET); 
			if ( nodes != null && nodes.getLength() > 0 ) {
				Element pubdate_elem = (Element) nodes.item(0);
				String yearStr = getSubtreeElementValueByTagName(pubdate_elem, "year");
				String dayStr = getSubtreeElementValueByTagName(pubdate_elem, "day");			//01, 14
				String monthStr = getSubtreeElementValueByTagName(pubdate_elem, "month");		//July?, or 07
				
				int yearDigit = Integer.parseInt(yearStr);
				setPublicationYear( yearDigit );
				
				//YYYY-MM-DD
				if ( monthStr == null ) {
					monthStr = "01";
				}
				if ( dayStr == null ) {
					dayStr = "01";
				}
				String pub_date_str = yearStr + "-" + monthStr + "-" + dayStr;
				setDatePublished( pub_date_str );
			}
			else {
				nodes = (NodeList)xPath.evaluate("/book/book-meta/pub-date", doc, XPathConstants.NODESET); 				//NOMOS
				if ( nodes != null && nodes.getLength() > 0 ) {
					Element pubdate_elem = (Element) nodes.item(0);
					String yearStr = getSubtreeElementValueByTagName(pubdate_elem, "year");
					String dayStr = getSubtreeElementValueByTagName(pubdate_elem, "day");			//01, 14
					String monthStr = getSubtreeElementValueByTagName(pubdate_elem, "month");		//July?, or 07
					
					int yearDigit = Integer.parseInt(yearStr);
					setPublicationYear( yearDigit );
					
					//YYYY-MM-DD
					if ( monthStr == null ) {
						monthStr = "01";
					}
					if ( dayStr == null ) {
						dayStr = "01";
					}
					String pub_date_str = yearStr + "-" + monthStr + "-" + dayStr;
					setDatePublished( pub_date_str );
				}
			}
		}
		
		//Amy: We don't want include pub year 0 books. 10/28/2021.
		if ( getPublicationYear()== 0 ) {
			throw new IllegalArgumentException("Pub year 0");
		}

		
	}



	/**
	 * https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/subj-group.html
	 * ((subject | compound-subject)+, subj-group*)
	 * @param doc
	 * @throws XPathExpressionException 
	 */
	private void setBookSourceCategoery(Document doc) throws XPathExpressionException {
		List<String> subj_list = new ArrayList<>();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate("//subject/named-content[@content-type='heading-text']", doc, XPathConstants.NODESET);  //all <subject> Elements 
		if ( nodes == null ) {
			nodes = (NodeList)xPath.evaluate("//subject", doc, XPathConstants.NODESET);  //all <subject> Elements 
		}
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element e = (Element) nodes.item(i);
			String subject = e.getTextContent().trim();
		    
		    if ( subject != null && ! subject.isEmpty()) {
		    	subj_list.add(subject);
		    }
		    	
		}

		
		NodeList nodes_1 = (NodeList)xPath.evaluate("//compound-subject-part[@content-type='text']", doc, XPathConstants.NODESET);  //all <compound-subject-part> Elements 
		for (int i = 0; i < nodes_1.getLength(); ++i) {
			Element e = (Element) nodes_1.item(i);
			String subject = e.getTextContent().trim();
		    
		    if ( subject != null && ! subject.isEmpty()) {
		    	subj_list.add(subject);
		    }
		}
		
		if ( subj_list != null && !subj_list.isEmpty()) {
			setSourceCategory(subj_list);
		}
		
		
		
	}


	/**
	 * Most common place is from book[@xml:lang]
	 * @param doc
	 * @throws XPathExpressionException 
	 */
	private void setBookLanguage(Document doc) throws XPathExpressionException {
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate("/book/@*[name()='xml:lang']", doc, XPathConstants.NODESET); 

		List<String> languages = new ArrayList<>();
		
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Node attrNode = (Node) nodes.item(i);
			String lang = attrNode.getTextContent();		//2 letter
			if ( lang != null ) {

				String lang3letter = TDMUtil.convertLan2ToLan3( lang );
				
				if ( lang3letter != null ) {
					languages.add(lang3letter);
				}

			}
		}
		setLanguage(  languages);
		
	}


	/**
	 * 
	 * @param doc
	 * @throws XPathExpressionException 
	 */
	private void setBookAbstract(Document doc) throws XPathExpressionException {
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-meta/abstract", doc, XPathConstants.NODESET); 

		if ( nodes != null && nodes.getLength() > 0 ) {
			Element e = (Element) nodes.item(0);
			String abs = e.getTextContent();
			setAbstractStr(abs);
		}
		
	}


	/*
	 * https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/counts.html
	 * (book-count*, book-fig-count?, book-table-count?, book-equation-count?, book-ref-count?, book-page-count?, book-word-count?)
	 * All bits xml files are using page-count
	 */
	private void setBookPageCount(Document doc) throws XPathExpressionException {
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-meta/counts/page-count", doc, XPathConstants.NODESET);  //All bits xml files are using page-count  instead of book-page-count
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element e = (Element) nodes.item(i);
		    String nodeName = e.getNodeName();
		    String countAttr = e.getAttribute("count");
		    
		    if ( countAttr != null && ! countAttr.isEmpty()) {
		    	int pagecount = new Integer(countAttr).intValue();
		    	setPageCount(pagecount);
		    }
		}

		
	}


	private void setBookURL(Document doc) {
		String doi = getDoi();
		
		if ( doi != null && ! doi.isEmpty()) {
			if ( !doi.startsWith("http")) {
				String url = "http://doi.org/" + doi;
				setUrl( url );
			}
			else {
				setUrl(url);
			}
		}
	}

	/*
	 * https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/isbn.html
	 * (#PCDATA | x)*
	 */
	private void setBookISBN(Document doc) throws XPathExpressionException{
		
		List<Identifier> idList = getIdentifiers();
		if ( idList == null ) {
			idList = new ArrayList<>();
		}
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-meta/isbn", doc, XPathConstants.NODESET);
		
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element e = (Element) nodes.item(i);
		    String isbnStr = e.getTextContent();
		    try {
				Identifier id = new Identifier("isbn", isbnStr);
				if ( !idList.contains(id)) {		//duplication check
			    	idList.add(id);
			    }
			} catch (Exception e1) {
				logger.error( programName + ":setBookISBN: " + isbnStr + " " + e1);
			}
		    
		}
		
		setIdentifiers(idList);

	}


	private void setBookDOI(Document doc)  throws XPathExpressionException {
		
		List<Identifier> idList = getIdentifiers();
		if ( idList == null ) {
			idList = new ArrayList<>();
		}
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-meta/book-id[@book-id-type='doi' or @pub-id-type='doi']",
		        doc, XPathConstants.NODESET);
		
		String doi = null;
		if ( nodes != null && nodes.getLength()>0 ) {
			doi = nodes.item(0).getTextContent();
			setDoi(doi);
		}
		
		//Add book's doi as one Identifier
		Identifier doi_id;
		try {
			doi_id = new Identifier("doi", doi);
			idList.add(doi_id);
			setIdentifiers(idList);
		} catch (Exception e) {
			
		}

		
	}


	/*
	 * See https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/contrib-group.html
	 * (contrib | address | aff | aff-alternatives | author-comment | bio | email | etal | ext-link | fn | on-behalf-of | role | uri | xref | x)+
	 * See https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/contrib.html
	 * (contrib-id | anonymous | collab | collab-alternatives | name | name-alternatives | string-name | degrees | address | aff | 
	 *                 aff-alternatives | author-comment | bio | email | etal | ext-link | fn | on-behalf-of | role | uri | xref | x)*
	 * Contributor Naming Elements
    	<anonymous> Anonymous
    	<collab> Collaborative (Group) Author
    	<collab-alternatives> Collaboration Alternatives
    	<name> Name of Person
    	<name-alternatives> Name Alternatives
    	<string-name> Name of Person (Unstructured)
	 */
	private void setBookCreator(Document doc) throws XPathExpressionException {
		
		//Evaluate XPath against Document itself
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList)xPath.evaluate("/book/book-meta/contrib-group/contrib",		//TODO test muse AU pbd16g5v916 multiple contrib-group
		        doc, XPathConstants.NODESET);
		
		creator = new ArrayList<>();
		for (int i = 0; i < nodes.getLength(); ++i) {
			Element e = (Element) nodes.item(i);
		    Person p = parseAContribNode( e );
		    if ( p == null ) {
		    	continue;
		    }
		    String p_name = p.getFullName();
		    if ( p != null ) {
		    	creator.add(p_name);
		    }
		}
		
	}


	/*
	 * https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/contrib.html
	 * (contrib-id | anonymous | collab | collab-alternatives | name | name-alternatives | string-name | degrees | address | aff 
	 * 		| aff-alternatives | author-comment | bio | email | etal | ext-link | fn | on-behalf-of | role | uri | xref | x)*
	 */
	private Person parseAContribNode(Element contrib_node) throws XPathExpressionException {
		Person creator = null;
		String firstName;
		String lastName;
		String nameString;		//use as full name
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		XPathExpression expr = xPath.compile("./*");
        NodeList contribChildrenNodes = (NodeList) expr.evaluate(contrib_node, XPathConstants.NODESET);
		
				
		for(int i =0; i< contribChildrenNodes.getLength(); i++) {
			Node e = contribChildrenNodes.item(i);
			String nodeName = e.getNodeName();

			if ( nodeName != null && nodeName.equals("string-name")) {
			
				//String name_stype_attr = e.getAttribute("name-style");
				firstName = getFirstChildElementValueByTagName(e, "given-names");
				lastName = getFirstChildElementValueByTagName(e, "surname");
				if ( firstName != null && lastName != null ) {
					creator = new Person(firstName, lastName, getArkId() );
				}
				else {
					nameString = e.getTextContent();
					creator = new Person(nameString, getArkId());
				}
				
			}
			else if ( nodeName != null && nodeName.equals("name")) {     //(((surname, given-names?) | given-names), prefix?, suffix?)
				firstName = getFirstChildElementValueByTagName(e, "given-names");
				lastName = getFirstChildElementValueByTagName(e, "surname");
				if ( firstName != null && lastName != null ) {
					creator = new Person(firstName, lastName, getArkId() );
				}
				else {
					nameString = e.getTextContent();
					creator = new Person(nameString, getArkId());
				}
			}
			else if ( nodeName != null && nodeName.equals("collab")) {
				nameString = e.getTextContent();
				creator = new Person( nameString, getArkId());
			}
			else if ( nodeName != null && nodeName.equals("aff")) {
				nameString = e.getTextContent();
				creator = new Person( nameString, getArkId());
			}
			
			if ( creator != null ) {
				break;
			}
		}
		return creator;
	}


	/*
	 * See https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/book-title.html
	 * (#PCDATA | email | ext-link | uri | inline-supplementary-material | related-article | related-object | 
	 * 		hr | bold | fixed-case | italic | monospace | overline | overline-start | overline-end | 
	 * 		roman | sans-serif | sc | serif | strike | underline | underline-start | underline-end | ruby | 
	 * 		alternatives | inline-graphic | private-char | chem-struct | inline-formula | tex-math | mml:math | abbrev |
	 * 		index-term | index-term-range-end | milestone-end | milestone-start | named-content | styled-content | 
	 * 		fn | target | xref | sub | sup | x | break)*
	 */
	private void setBookTitle(Document doc) throws XPathExpressionException {
		
		//Evaluate XPath against Document itself
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList titlenodes = (NodeList)xPath.evaluate("/book/book-meta/book-title-group/book-title",
		        doc, XPathConstants.NODESET);
		
		if ( titlenodes != null && titlenodes.getLength() > 0 ) {
			Element e = (Element) titlenodes.item(0);
		    String book_title = e.getTextContent().trim();
		    setTitle(book_title);
		}
		
		xPath = XPathFactory.newInstance().newXPath();
		NodeList subtitlenodes = (NodeList)xPath.evaluate("/book/book-meta/book-title-group/subtitle",
		        doc, XPathConstants.NODESET);
		
		if ( subtitlenodes != null && subtitlenodes.getLength() > 0 ) {
			Element e = (Element) subtitlenodes.item(0);
		    String subtitle = e.getTextContent().trim();
		    setSubTitle(subtitle);
		}
		
	}


	/*
	 * If provider is not MUSE, use provider as publisher. 
	 * Otherwise 
	 * See https://jats.nlm.nih.gov/extensions/bits/tag-library/2.0/element/publisher.html
	 * ((publisher-name, publisher-loc?)+)
	 */
	private void setBookPublisher(Document doc) throws Exception {
		Publisher publisher = null;
		
		String providerId = getPublisherId();
		
		if ( providerId == null ) {
			logger.debug( "No provider ID has been set");
			throw new Exception("Provider ID not set yet");
		}
		
		if ( providerId.equals("MUSE") ) {
			//use publisher info from xml file
			XPath xPath = XPathFactory.newInstance().newXPath();
			NodeList nodes = (NodeList)xPath.evaluate("/book/book-meta/publisher", doc, XPathConstants.NODESET);
			Element publisher_elem = (Element) nodes.item(0);
			
			String publisher_name = getSubtreeElementValueByTagName( publisher_elem, "publisher-name");
			setPublisher(publisher_name);
			
		}
		else {
			//use Portico's provider as publisher
			try {
				publisher = new Publisher(providerId);  //get publisher info from cmi_provider table. But no address in cmi_provider table.
			} catch (Exception e) {
				logger.error( programName + ":setBookPublisher " + e.getMessage());
				throw e;
			}		
			String provider_name = publisher.getName();

			setPublisher(provider_name);
		
		}
		
	}

	/**
	 * Perform book level or chapter level NLP tasks.
	 * @throws Exception
	 */
	public void performBookNLPTask() throws Exception {
		
		String bookAuId = getAuid();
		String providerId = getPublisherId();
		String subDir = getSubDir();
		
		List<V2Chapter> chapters = getChapters();
		
		//book level NLP task
		List<String> bookFullTextByPage = null;
		if ( isSinglePdf() || getPublisherId().equals("SAGE")) {
			String bookpdf = getPdfFiles().get(0);
			String pdfFileWithPath = inputDir + File.separator + subDir + File.separator + 
								bookAuId.substring(bookAuId.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + bookpdf;

			bookFullTextByPage = TDMUtil.getFullTextFromPDF( pdfFileWithPath );
			setFullText( bookFullTextByPage);
			if ( bookFullTextByPage != null && ! bookFullTextByPage.isEmpty()) {
				if ( getPageCount() == 0 ) {
					setPageCount( bookFullTextByPage.size());
				}
				setFulltextFrom("pdf");
			}
			else {
				setFulltextFrom("no fulltext");
			}
			
			//join page text into 1 string
			String fulltext = String.join("\n", bookFullTextByPage); 
			
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
	 
			return;
		}

		int bookPageCount = 0;
		processedChapterCount = 0;
		for(int i= 0; i< chapters.size(); i++) {
			V2Chapter chapter = chapters.get(i);
			String chapterTitle = chapter.getTitle();
			
			try {
				chapter.performChapterNLPTask();
				bookPageCount += chapter.getPageCount();
				processedChapterCount++;
			} catch (Exception e) {
				logger.error( programName + ":performBookNLPTask :performChapterNLPTask :Error in " + providerId + " " +
						bookAuId + " on chapter " + chapterTitle + " " + e );
			}        
		}
		
		setChapters(chapters);
		if ( getPageCount() == 0 ) {
			setPageCount( bookPageCount );
		}
		
	}

	/**
	 * Insert or update tdm_book entry for book and chapters.
	 */
	public void logToDB() {
		
		String csname = getContentSetName();
		String au_id = getAuid();
		String su_id = getBitsXmlFileSuId();
		String publisherid = getPublisherId();
		int pub_year = getPublicationYear();
		
		String pdfFile = null;
		if ( isSinglePdf() ) {
			pdfFile = getPdfFiles().get(0);
		}
		else if ( getPdfFiles()!= null ) {
			pdfFile = getPdfFiles().toString();
			if ( pdfFile.length() >= 500 ) {
				pdfFile = pdfFile.substring(0, 490) + " ...]";
			}
		}
		else {
			pdfFile = "";
		}
		
		String xmlFile = getXmlFiles() == null? null: getXmlFiles().toString();
		String oa = "";
		
		if ( isFullTextAvailable() ) {
			oa = "Y";
		}
		else {
			oa = "N";
		}
		
		String existance_query = "select count(*) from tdm_book where au_id='" + au_id + "' and type='book'";
		
		String insert_query = "insert into TDM_BOOK ( au_id, su_id, publisher_id, type, content_set_name, "
				+ " pdf_file, xml_file, oa, pub_year, creation_ts ) values (?,?,?,?,?,?,?,?,?,?)";
		
		boolean newInsert = true;
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement existance_stmt = conn.createStatement();
				PreparedStatement insert_stmt = conn.prepareStatement(insert_query);
				Statement update_stmt = conn.createStatement(); ) {
			
			ResultSet rs = existance_stmt.executeQuery(existance_query);
			
			if (rs.next()) {
				int count = rs.getInt(1);
				
				if ( count >= 1 ) {
					newInsert = false;
				}
			}
			
			rs.close();
			
			if ( newInsert ) {	//this au has not been logged before

				insert_stmt.setString(1, au_id);
				insert_stmt.setString(2, su_id);			
				insert_stmt.setString(3, publisherid);
				insert_stmt.setString(4, "book");
				insert_stmt.setString(5, csname);
				insert_stmt.setString(6, pdfFile);
				insert_stmt.setString(7, xmlFile);
				insert_stmt.setString(8,  oa);
				insert_stmt.setInt(9, pub_year);
				Timestamp sqlDate = new java.sql.Timestamp( new java.util.Date().getTime());
				insert_stmt.setTimestamp(10, sqlDate);

				insert_stmt.executeUpdate();

			}
			else {	//this au has been inserted before
				
				String update_query = "update tdm_book set publisher_id='" + publisherid + "', type='book', content_set_name=q'$" + csname + "$', ";
				if ( pdfFile != null ) {
					update_query += "pdf_file='" + pdfFile + "', ";
				}
				if ( xmlFile != null ) {
					update_query += "xml_file='" + xmlFile + "', ";
				}

				update_query += "oa='" + oa + "', pub_year=" + pub_year + ", modification_ts=current_timestamp where au_id='" + au_id + "'  and type='book'";

				update_stmt.executeQuery(update_query);
			}
		}
		catch(  Exception  e) {
				
			logger.error( programName + ":logToDB:  " + e);
			e.printStackTrace();
		}
		
		
		List<V2Chapter> chapters = getChapters();
		
		
		if ( chapters == null || chapters.isEmpty()) {
			return;
		}
		
		for(V2Chapter chapter: chapters) {
			try {
				chapter.logToDB();
			}
			catch(Exception e) {
				logger.error( programName + ":logToDB :Error log Chapter to DB " + publisherid + " " + au_id + " "  + e);
			}
		}
		
	}

	
	/**
	 * Create book json file if chapters do not exist. Otherwise create chapter json files.
	 * @throws IOException
	 */
	public void outputJson() throws IOException {
		
		String outputDir = getOutputDir();

		//create output directory if it doesn't exist
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

		//output book json file if chapters is null
		List<V2Chapter> chapters = getChapters();
		
		if ( chapters == null || chapters.isEmpty()) {
			
			String jsonStr = toJsonStr( prettyFlag );
			String auid = getAuid();
			String filename = auid.substring( auid.lastIndexOf("/") + 1) + ".json";		//ark:/27927/pgj2j7b70gv --> pgj2j7b70gv.json
	        List<String> lines = Arrays.asList(jsonStr);
	        
	        Path file = Paths.get( outputDir + File.separator + getSubDir() + File.separator + filename );
	        
	        try {
				Files.write(file, lines, Charset.forName("UTF-8"));
			} catch (IOException e) {
				logger.error( programName + ":outputJson: Error writing to " + file.toString() + " " + e.getMessage());
				e.printStackTrace();
				throw e;
			}
			
			return;
		}
		
		//else output chapter json files
		for(V2Chapter chapter: chapters) {
			try {
				chapter.outputJson();
			}
			catch(Exception e) {
				logger.error(programName + ":outputJson: Error writing chapter to json file " + e.getMessage());
			}
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


	/**
	 * Select the first children element by tag name.
	 * @param e
	 * @param string
	 * @return
	 * @throws XPathExpressionException 
	 */
	private String getFirstChildElementValueByTagName(Node node, String tag) throws XPathExpressionException {
		String nodeValue = null;

		XPath xPath = XPathFactory.newInstance().newXPath();
		XPathExpression expr = xPath.compile("./" + tag + "[1]");
        NodeList resultNodes = (NodeList) expr.evaluate(node, XPathConstants.NODESET);
		
		if(resultNodes != null &&  resultNodes.getLength()> 0 ) {
			Node e =  resultNodes.item(0);
			nodeValue = e.getTextContent();
		}
		
		return nodeValue;
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



	/**
	 * 
	 * @param start_elem
	 * @param xPathString
	 * @return First result text value
	 * @throws XPathExpressionException 
	 */
	private String getSubtreeNodeValueByXPath(Element start_elem, String xPathString) throws XPathExpressionException {
		String nodeValue = null;

		XPath xPath = XPathFactory.newInstance().newXPath();
		//XPathExpression expr = xPath.compile(".//" + xPathString);
		XPathExpression expr = xPath.compile("descendant-or-self::" + xPathString);
        NodeList resultNodes = (NodeList) expr.evaluate(start_elem, XPathConstants.NODESET);
		
		if(resultNodes != null &&  resultNodes.getLength()> 0 ) {
			Node e =  resultNodes.item(0);
			nodeValue = e.getTextContent();
		}
		
		return nodeValue;
	}



	public String getSubDir() {
		return subDir;
	}

	public void setSubDir(String subDir) {
		this.subDir = subDir;
	}

	public List<String> getPdfFiles() {
		return pdfFiles;
	}

	public void setPdfFiles(List<String> pdfFiles) {
		this.pdfFiles = pdfFiles;
	}

	public List<String> getXmlFiles() {
		return xmlFiles;
	}

	public void setXmlFiles(List<String> xmlFiles) {
		this.xmlFiles = xmlFiles;
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

	public String getFulltextFrom() {
		return fulltextFrom;
	}

	public void setFulltextFrom(String fulltextFrom) {
		this.fulltextFrom = fulltextFrom;
	}

	public UniBook getUnibook() {
		return unibook;
	}

	public void setUnibook(UniBook unibook) {
		this.unibook = unibook;
	}

	public String getBitsXmlFileName() {
		return bitsXmlFileName;
	}

	public void setBitsXmlFileName(String bitsXmlFileName) {
		this.bitsXmlFileName = bitsXmlFileName;
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

	public int getProcessedChapterCount() {
		return processedChapterCount;
	}

	public void setProcessedChapterCount(int processedChapterCount) {
		this.processedChapterCount = processedChapterCount;
	}

	public String getBitsXmlFileSuId() {
		return bitsXmlFileSuId;
	}

	public void setBitsXmlFileSuId(String bitsXmlFileSuId) {
		this.bitsXmlFileSuId = bitsXmlFileSuId;
	}



	public boolean isSinglePdf() {
		return singlePdf;
	}

	public List<V2Chapter> getChapters() {
		return chapters;
	}

	public void setChapters(List<V2Chapter> chapters) {
		this.chapters = chapters;
	}

	public void setSinglePdf(boolean singlePdf) {
		this.singlePdf = singlePdf;
	}


}

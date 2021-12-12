package org.portico.tdm.tdm2.tools;


import static java.util.Arrays.asList;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.SourceLocator;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageTree;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDDocumentOutline;
import org.portico.tdm.tdm2.datawarehouse.DWArticle;
import org.portico.tdm.tdm2.datawarehouse.DWIssue;
import org.portico.tdm.tdm2.datawarehouse.DWJournal;
import org.portico.tdm.tdm2.ebook.UniBook;


import org.portico.tdm.tdm2.schemaorg.Person;
import org.portico.tdm.tdm2.tdmv2.TokenCount;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.optimaize.langdetect.DetectedLanguage;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetectorFactory;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.langdetect.LanguageDetectorSampleStream;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorFactory;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.sentdetect.SentenceSample;
import opennlp.tools.sentdetect.SentenceSampleStream;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.InputStreamFactory;
import opennlp.tools.util.MarkableFileInputStreamFactory;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import opennlp.tools.util.Span;
import opennlp.tools.util.TrainingParameters;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

//import com.asprise.util.pdf.PDFReader;
//import com.asprise.util.ocr.OCR;


public class TDMUtil {
	
	private static final String[] PERSON_STOP_NAMES = new String[] { "The", "They", "This", "That", "I", "He", "She", "It" };
	private static final Set<String> PERSON_STOP_SET = new HashSet<>(Arrays.asList(PERSON_STOP_NAMES));
	private static final Object[] EMPTY_ARRAY = new Object[0];
	
	private static final String ldap_name_attr_noid_buffer_size = "1";
	private static final String ldap_name_attr_noid_prefix_prod = "27927/phz";
	private static final String ldap_name_attr_noid_prefix_dev = "27927/dfd";			//old value: "27927/dbd";
	private static final String ldap_name_attr_noid_url_prod = "http://pr2ptgpprd01.ithaka.org:8810/noid/noid_phz.sh";
	private static final String ldap_name_attr_noid_url_dev = "http://pr2ptgpdev03.ithaka.org:8810/noid/noid_dfd.sh"; 	//old value:"http://pr2cmdev02.ithaka.org:5510/noid/noid.sh";

	private static final String Jdbc_archive_User="analytics";
	private static final String Jdbc_archive_Password="analytics1";
	private static final String Jdbc_archive_Url="jdbc:oracle:thin:@(description = (address_list = (address = (protocol = tcp)(host =pr2ptcingestora02.ithaka.org)(port = 1525)))(connect_data = (SID = pparch1) (server =dedicated)))";

	private static final String Jdbc_dw_User="analytics";
	private static final String Jdbc_dw_Password="analytics1";
	private static final String Jdbc_dw_Url="jdbc:oracle:thin:@(description = (address_list = (address = (protocol = tcp)(host =pr2ptcingestora02.ithaka.org)(port = 1525)))(connect_data = (SID = pparch1) (server =dedicated)))";

	private static final String Jdbc_dev_User="analytics_dev";
	private static final String Jdbc_dev_Password="analytics_dev1";
	private static final String Jdbc_dev_Url="jdbc:oracle:thin:@(description = (address_list = (address = (protocol = tcp)(host =pr2ptcingestora02.ithaka.org)(port = 1525)))(connect_data = (SID = pparch1) (server =dedicated)))";

	private static Map<String, String> Lan2ToLan3Map;
	static {
		String[] Languages = Locale.getISOLanguages();	//all lower cases
		Lan2ToLan3Map = new HashMap<>();
	    for (String lan2 : Languages) {
	        Locale locale = new Locale(lan2);
	        Lan2ToLan3Map.put(lan2, locale.getISO3Language());
	    }
	}
	
	
	private static final Map<String, String> charMap;
	
	static
    {
        charMap = new HashMap<String, String>();
        charMap.put("\u00E2\u0082\u00AC", "\u20AC");
        charMap.put("\u00E2\u20AC\u009A", "\u201A");
        charMap.put("\u00E2\u20AC\u0098", "\u2018");
        charMap.put("\u00E2\u20AC\u0099", "\u2019");
        charMap.put("\u00E2\u20AC\u009C", "\u201C");
        charMap.put("\u00E2\u20AC\u009D", "\u201D");
        charMap.put("\u00E2\u20AC\u00A6", "\u2026");
        charMap.put("\u00E2\u20AC\u201C", "\u2013");
        charMap.put("\u00E2\u20AC\u201D", "\u2014");
        charMap.put("\u00C5\u00A1", "\u0161");
        //charMap.put("", "\u010D");
        charMap.put("\u00C5\u00B8", "\u0178");
        charMap.put("\u00C2\u00A3", "\u00A3");
        charMap.put("\u00C2\u00A6", "|");
        charMap.put("\u00C2\u00A9", "\u00A9");
        charMap.put("\u00C2\u00AE", "\u00AE");
        charMap.put("\u00C2\u00B1", "\u00B1");
        charMap.put("\u00C2\u00B2", "\u00B2");
        charMap.put("\u00C3\u0080", "\u00C0");
        charMap.put("\u00C3\u0082", "\u00C2");
        charMap.put("\u00C3\u0083", "\u00C3");
        charMap.put("\u00C3\u0084", "\u00C4");
        charMap.put("\u00C3\u0085", "\u00C5");
        charMap.put("\u00C3\u0086", "\u00C6");
        charMap.put("\u00C3\u0087", "\u00C7");
        charMap.put("\u00C3\u0088", "\u00C8");
        charMap.put("\u00C3\u2030", "\u00C9");
        charMap.put("\u00C3\u008A", "\u00CA");
        charMap.put("\u00C3\u008B", "\u00CB");
        charMap.put("\u00C3\u008C", "\u00CC");
        charMap.put("\u00C3\u008E", "\u00CE");
        charMap.put("\u00C3\u0091", "\u00D1");
        charMap.put("\u00C3\u0092", "\u00D2");
        charMap.put("\u00C3\u0093", "\u00D3");
        charMap.put("\u00C3\u0094", "\u00D4");
        charMap.put("\u00C3\u0095", "\u00D5");
        charMap.put("\u00C3\u0096", "\u00D6");
        charMap.put("\u00C3\u0097", "\u00D7");
        charMap.put("\u00C3\u0099", "\u00D9");
        charMap.put("\u00C3\u009A", "\u00DA");
        charMap.put("\u00C3\u009B", "\u00DB");
        charMap.put("\u00C3\u009C", "\u00DC");
        charMap.put("\u00C3\u00A1", "\u00E1");
        charMap.put("\u00C3\u00A2", "\u00E2");
        charMap.put("\u00C3\u00A3", "\u00E3");
        charMap.put("\u00C3\u00A4", "\u00E4");
        charMap.put("\u00C3\u00A5", "\u00E5");
        charMap.put("\u00C3\u00A6", "\u00E6");
        charMap.put("\u00C3\u00A7", "\u00E7");
        charMap.put("\u00C3\u00A8", "\u00E8");
        charMap.put("\u00C3\u00A9", "\u00E9");
        charMap.put("\u00C3\u00AA", "\u00EA");
        charMap.put("\u00C3\u00AB", "\u00EB");
        charMap.put("\u00C3\u00AC", "\u00EC");
        charMap.put("\u00C3\u00AD", "\u00ED");
        charMap.put("\u00C3\u00AE", "\u00EE");
        charMap.put("\u00C3\u00AF", "\u00EF");
        charMap.put("\u00C3\u00B0", "\u00F0");
        charMap.put("\u00C3\u00B1", "\u00F1");
        charMap.put("\u00C3\u00B2", "\u00F2");
        charMap.put("\u00C3\u00B3", "\u00F3");
        charMap.put("\u00C3\u00B4", "\u00F4");
        charMap.put("\u00C3\u00B5", "\u00F5");
        charMap.put("\u00C3\u00B6", "\u00F6");
        charMap.put("\u00C3\u00B7", "\u00F7");
        charMap.put("\u00C3\u00B8", "\u00F8");
        charMap.put("\u00C3\u00B9", "\u00F9");
        charMap.put("\u00C3\u00BA", "\u00FA");
        charMap.put("\u00C3\u00BB", "\u00FB"); 
        charMap.put("\u00C3\u00BC", "\u00FC");
        charMap.put("\u00C3\u00BD", "\u00FD");
        charMap.put("\u00C3\u00BE", "\u00FE");
        charMap.put("\u00C3\u00BF", "\u00FF");
    }
	
	
	private static final String PLAIN_ASCII =
		      "AaEeIiOoUu"    // grave
		    + "AaEeCcIiOoUuYySs"  // acute
		    + "AaEeIiOoUuYy"  // circumflex
		    + "AaOoNn"        // tilde
		    + "AaEeIiOoUuYy"  // umlaut
		    + "Aa"            // ring
		    + "CcSs"            // cedilla
		    + "OoUu"          // double acute
		    + "Oo"			  //stroke
		    + "AaGg"			//breve
		    + "AaEeIiOooUuYyGg"	//macron
		    ;

	private static final String UNICODE =
		     "\u00C0\u00E0\u00C8\u00E8\u00CC\u00EC\u00D2\u00F2\u00D9\u00F9"             
		    + "\u00C1\u00E1\u00C9\u00E9\u0106\u0107\u00CD\u00ED\u00D3\u00F3\u00DA\u00FA\u00DD\u00FD\u015A\u015B" 
		    + "\u00C2\u00E2\u00CA\u00EA\u00CE\u00EE\u00D4\u00F4\u00DB\u00FB\u0176\u0177" 
		    + "\u00C3\u00E3\u00D5\u00F5\u00D1\u00F1"
		    + "\u00C4\u00E4\u00CB\u00EB\u00CF\u00EF\u00D6\u00F6\u00DC\u00FC\u0178\u00FF" 
		    + "\u00C5\u00E5"                                                             
		    + "\u00C7\u00E7\u015E\u015F" 
		    + "\u0150\u0151\u0170\u0171" 
		    + "\u00D8\u00F8"
		    + "\u0102\u0103\u011E\u011F"
		    + "\u0100\u0101\u0112\u0113\u012A\u012B\u014C\u014D\u0304\u016A\u016B\u0232\u0233\u1E20\u1E21"
		    ;

	static HashMap<String, List<String>> language_title_sort_articles = new HashMap<String, List<String>>();
	static {
		language_title_sort_articles.put("eng", new ArrayList<>(asList("^A\\s+", "^The\\s+", "^An\\s+")) );   // English
		language_title_sort_articles.put("spa", new ArrayList<>(asList("^El\\s+", "^La\\s+", "^Lo\\s+", "^Los\\s+", "^Las\\s+", "^Un\\s+", "^Una\\s+", "^Unos\\s+", "^Unas\\s+")) );   // Spanish 
		language_title_sort_articles.put("fra", new ArrayList<>(asList("^Le\\s+", "^La\\s+", "^L'", "^Les\\s+", "^Une\\s+", "^Un\\s+", "^Des\\s+", "^De\\s+La\\s+", "^De\\s+", "^D'")) );   //  French
		language_title_sort_articles.put("ita", new ArrayList<>(asList("^Lo\\s+", "^Il\\s+", "^L'", "^La\\s+", "^Gli\\s+", "^I\\s+", "^Le\\s+")) );   //  Italian
		language_title_sort_articles.put("por", new ArrayList<>(asList("^A\\s+", "^O\\s+", "^Os\\s+", "^As\\s+", "^Um\\s+", "^Uns\\s+", "^Uma\\s+", "^Umas\\s+")) );   //  Portuguese
		language_title_sort_articles.put("ron", new ArrayList<>(asList("^Un\\s+", "^O\\s+", "^Ni\u015Fte\\s+")) );   //  Romanian  //LATIN SMALL LETTER S WITH CEDILLA \u015F or 351
		language_title_sort_articles.put("deu", new ArrayList<>(asList("^Der\\s+", "^Die\\s+", "^Das\\s+", "^Den\\s+", "^Ein\\s+", "^Eine\\s+", "^Einen\\s+", 
																		"^Dem\\s+", "^Des\\s+", "^Einem\\s+","^Eines\\s+")) );   // German 
		language_title_sort_articles.put("nld", new ArrayList<>(asList("^De\\s+", "^Het\\s+", "^Een\\s+", "n\\s+", "s\\s+", "^Ene\\s+", "^Ener\\s+", "^Enes\\s+", 
																		"^Den\\s+", "^Der\\s+", "^Des\\s+", "^'t\\s+")) );   //  Dutch
		language_title_sort_articles.put("swe", new ArrayList<>(asList("^En\\s+", "^Ett\\s+", "^Det\\s+", "^Den\\s+", "^De\\s+")) );   //  Swedish
		language_title_sort_articles.put("tur", new ArrayList<>(asList("^Bir\\s+")) );   //  Turkish
		language_title_sort_articles.put("afr", new ArrayList<>(asList("^'n\\s+", "^Die\\s+")) );   //  Afrikans
		language_title_sort_articles.put("ell", new ArrayList<>(asList("^O\\s+", "^I\\s+", "^To\\s+", "^Ta\\s+", "^Tus\\s+", "^Tis\\s+", "^'Enas\\s+", "^'Mia\\s+", "^'Ena\\s+", "^'Enan\\s+")) );   //  Greek
		language_title_sort_articles.put("hun", new ArrayList<>(asList("^A\\s+", "^Az\\s+", "^Egy\\s+")) );   //  Hungarian
		language_title_sort_articles.put("lat", new ArrayList<>(asList("")) );   //  Latin has no article
		//language_title_sort_articles.put("", new ArrayList<>(asList("\\s+", "\\s+", "\\s+", "\\s+", "\\s+")) );   //  
		
	       
	}
	
	
	static Logger logger = LogManager.getLogger(TDMUtil.class.getName());
	static String programName = "TDMUtil";
	
	static String inputDir = "input";
	String inputSubDir;
	
	private static ComboPooledDataSource dataSource = null;
	static {
		java.util.logging.Logger.getLogger("org.apache.pdfbox").setLevel(java.util.logging.Level.OFF);
	}
	
	public static void main(String[] args) {
		
		//get full text from PDF file
		//String pdffile = "input\\pgj239fsttr\\data\\pgj239f6s3f.pdf";			//Cambridge ISSN_02613409 (Archaeologia) v1 n1 AU_ID: ark:/27927/pgj239fsttr, published in 1770.
		//String pdffile = "input\\pfnm98twh\\data\\pfnm99k45.pdf";
		//String pdffile = "input\\ISSN_07115075\\phx58nrrgb5\\data\\phx58nrnsgk.pdf";
		String pdffile = "input\\begell\\ISSN_00188166\\pgg1hg82mq8\\data\\pgg1hg7wjk5.pdf";
		
		System.out.println("Get fulltext from pdf file: " + pdffile);
		try {
			String text = TDMUtil.getFulltextFromPDF(pdffile);
			System.out.println(text);
			
			List<String> lines = Arrays.asList(text);
	        Path file = Paths.get( "pdf_text.txt" );
	        try {
				Files.write(file, lines, Charset.forName("UTF-8"));
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		
		System.out.println("************End of full text ****************\n");
		
		System.out.println("*********** Split pdf into pages **********************\n");
		PDDocument document = null;
		try {
			document = PDDocument.load(new File(pdffile));
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		Splitter splitter = new Splitter();
		try {
			List<PDDocument> splittedDocuments = splitter.split(document);
			int index = 1;
			for(PDDocument doc: splittedDocuments) {
				try {
					String text = new PDFTextStripper().getText(doc);
					System.out.println("*********Page " + index++ + "**************");
					System.out.println(text);
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
			}

		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}

		System.out.println("****************************");
		System.out.println("***************** Split text by word count(500) *************");

		String inputText = SimpleXML2Text.getFullText("pfnm98twh\\data\\pfnm9p34f.xml");
		
		List<String> pages = TDMUtil.manaulSplitTextToPages("ISSN_17441374", "ark:/27927/pfnm98twh", inputText);
		
		
		
		/*PDFReader reader = new PDFReader(new File("my.pdf"));
		reader.open(); // open the file. 
		int pages = reader.getNumberOfPages();
		 
		for(int i=0; i < pages; i++) {
		   BufferedImage img = reader.getPageAsImage(i);
		   
		   // recognizes both characters and barcodes
		   String text = new OCR().recognizeAll(image);
		   System.out.println("Page " + i + ": " + text); 
		}
		 
		reader.close(); // finally, close the file.
*/
		
		//Count lines in this string
		int lineCount = TDMUtil.countNewLines( inputText );
		System.out.println("-------Count lines : -----");
		System.out.println(" Total lines in the text : " + lineCount);
		
		//Tokenize
		try {
			System.out.println("-------Tokenize text : -----");
			String[] tokens = TDMUtil.OpenNLPTokenize( inputText );
			System.out.println( "Total count of tokens: " + tokens.length );
			//for (String a : tokens)  {  System.out.print(a + " ");  }
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		//Language detect
		try {
			System.out.println("---------Predicted languages :-----------");
			Language[] languages = TDMUtil.detectLanguage1( inputText );
			
	        for(Language language:languages){
	            System.out.println(language.getLang()+"  confidence:"+language.getConfidence());
	        }
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		
		
		//Sentence detect
		String[] sentences = null;
		try {
			sentences = TDMUtil.detectSentence(inputText);
			
			System.out.println("-------- Detected sentences (" + sentences.length + "):");
			int count = 0;
			for (String sentence : sentences) {
				System.out.println( ++count + ", " + sentence);
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		//POS tag
    	TDMUtil.addPOSTag(sentences[80]);
    	
        // find person name
		try {
			System.out.println("-------Finding entities belonging to category : person name------");
			Span nameSpans[] = TDMUtil.findNames( inputText );
			System.out.println("Found " + nameSpans.length + " person names ");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

        // find place
        try {
            System.out.println("-------Finding entities belonging to category : place name------");
            Span nameSpans[] = TDMUtil.findLocations( inputText );
			System.out.println("Found " + nameSpans.length + " place names ");
        } catch (IOException e) {
            e.printStackTrace();
        }

	}
	
	
	static Language[] detectLanguage1(String inputText) throws IOException {
		LanguageDetectorModel model = null;
		
		 // loading the training data to LanguageDetectorSampleStream
        LanguageDetectorSampleStream sampleStream = null;
        try {
            InputStreamFactory dataIn = new MarkableFileInputStreamFactory(new File("input" + File.separator + "training-data" + File.separator + "DoccatSample.txt"));
            ObjectStream<String> lineStream = new PlainTextByLineStream(dataIn, "UTF-8");
            sampleStream = new LanguageDetectorSampleStream(lineStream);
        } catch (FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
 
        // training parameters
        TrainingParameters params = new TrainingParameters();
        params.put(TrainingParameters.ITERATIONS_PARAM, 100);
        params.put(TrainingParameters.CUTOFF_PARAM, 5);
        params.put("DataIndexer", "TwoPass");
        params.put(TrainingParameters.ALGORITHM_PARAM, "NAIVEBAYES");
 
        // train the model
        model = LanguageDetectorME.train(sampleStream, params, new LanguageDetectorFactory());

        //System.out.println("Completed");
 
        // load the model
        opennlp.tools.langdetect.LanguageDetector ld = new LanguageDetectorME(model);
        // use model for predicting the language
        Language[] languages = ld.predictLanguages(inputText);
        /*System.out.println("Predicted languages..");
        for(Language language:languages){
            // printing the language and the confidence score for the test data to belong to the language
            System.out.println(language.getLang()+"  confidence:"+language.getConfidence());
        }*/
        
        return languages;
	}
	
	public static String detectLanguage(String inputText) throws IOException {
		String languages = null;
		
		//load all languages:
		List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

		//build language detector:
		LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
				.withProfiles(languageProfiles)
				.build();

		//create a text object factory
		TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

		//query:
		TextObject textObject = textObjectFactory.forText(inputText);
		com.google.common.base.Optional<LdLocale> lang = languageDetector.detect(textObject);
		//DetectedLanguage dl = languageDetector.getProbabilities(textObject).get(0);
		
		//System.out.println("--------- Detected language " + lang.toString() + ":" + dl.getProbability());
		 //"DetectedLanguage["+ locale + ":" + probability+"]"
		if (lang.isPresent() ) {
			languages = lang.get().getLanguage();
		}
		 
		
		return languages;
	}


	public static void addPOSTag( String sentence ) {
		InputStream tokenModelIn = null;
		InputStream posModelIn = null;

		try {

			// tokenize the sentence
			//System.out.println("----------- AddPOSTag to sentent : --------------");
			//System.out.println(sentence);
			String tokens[] = OpenNLPTokenize(sentence);


			// Parts-Of-Speech Tagging
			// reading parts-of-speech model to a stream 
			posModelIn = new FileInputStream("resource/en-pos-maxent.bin");
			// loading the parts-of-speech model from stream
			POSModel posModel = new POSModel(posModelIn);
			// initializing the parts-of-speech tagger with model 
			POSTaggerME posTagger = new POSTaggerME(posModel);
			// Tagger tagging the tokens
			String tags[] = posTagger.tag(tokens);
			// Getting the probabilities of the tags given to the tokens
			/*double probs[] = posTagger.probs();

			System.out.println("Token\t:\t\tTag\t:\tProbability\n---------------------------------------------");
			for(int i=0;i<tokens.length;i++){
				System.out.println(tokens[i]+"\t:\t"+tags[i]+"\t:\t"+probs[i]);
			}*/

		}
		catch (IOException e) {
			// Model loading failed, handle the error
			e.printStackTrace();
		}
		finally {
			if (tokenModelIn != null) {
				try {
					tokenModelIn.close();
				}
				catch (IOException e) {
				}
			}
			if (posModelIn != null) {
				try {
					posModelIn.close();
				}
				catch (IOException e) {
				}
			}
		}
	}
	
	public static int countNewLines(String text) {
		/* Same result as down one.
		 if (isNullOrEmpty(text) ) {
	        return 0;
	    }
	    int count = 0;
	    int idx = 0;
	    String sub = "\n";
	    
	    while ((idx = text.indexOf(sub, idx)) != -1) {
	        count++;
	        idx += sub.length();
	    }*/
		
		int count=0;
		Pattern regex = Pattern.compile("^.*?[\\n\\r]", Pattern.MULTILINE);
		Matcher matcher = regex.matcher(text);
		while (matcher.find())
		{
			count++;
		}
		
	    return count;
	}
	

	public static int countEmptyLines(String text) {
		
		int count=0;
		Pattern regex = Pattern.compile("^\\s*\\n", Pattern.MULTILINE);
		Matcher matcher = regex.matcher(text);
		while (matcher.find())
		{
			count++;
		}
	    return count;
	}

	

	public static Span[] findLocations(String inputText) throws IOException {
		
		InputStream is = new FileInputStream("resource/en-ner-location.bin");
		 
        // load the model from file
        TokenNameFinderModel model = new TokenNameFinderModel(is);
        is.close();
 
        // feed the model to name finder class
        NameFinderME nameFinder = new NameFinderME(model);

        InputStream inputStream = new FileInputStream("resource/en-token.bin"); 
        TokenizerModel tokenModel = new TokenizerModel(inputStream); 
         
        //Instantiating the TokenizerME class 
        TokenizerME tokenizer = new TokenizerME(tokenModel); 
         
        //Tokenizing the given raw text 
        String tokens[] = tokenizer.tokenize( inputText );      
 
        Span nameSpans[] = nameFinder.find(tokens);
 
        // nameSpans contain all the possible entities detected
        /*for(Span s: nameSpans){
            System.out.print(s.toString());
            System.out.print("  :  ");
            // s.getStart() : contains the start index of possible name in the input string array
            // s.getEnd() : contains the end index of the possible name in the input string array
            for(int index=s.getStart();index<s.getEnd();index++){
                System.out.print(tokens[index]+" ");
            }
            System.out.println();
        }
        */
        return nameSpans;
	}

	public static Span[] findNames(String inputText) throws IOException {
		
		InputStream is = new FileInputStream("resource/en-ner-person.bin");
		 
        // load the model from file
        TokenNameFinderModel model = new TokenNameFinderModel(is);
        is.close();
 
        // feed the model to name finder class
        NameFinderME nameFinder = new NameFinderME(model);

        InputStream inputStream = new FileInputStream("resource/en-token.bin"); 
        TokenizerModel tokenModel = new TokenizerModel(inputStream); 
         
        //Instantiating the TokenizerME class 
        TokenizerME tokenizer = new TokenizerME(tokenModel); 
         
        //Tokenizing the given raw text 
        String tokens[] = tokenizer.tokenize( inputText );       

        Span nameSpans[] = nameFinder.find(tokens);
 
        // nameSpans contain all the possible entities detected
       /* for(Span s: nameSpans){
            System.out.print(s.toString());
            System.out.print("  :  ");
            // s.getStart() : contains the start index of possible name in the input string array
            // s.getEnd() : contains the end index of the possible name in the input string array
            for(int index=s.getStart();index<s.getEnd();index++){
                System.out.print(tokens[index]+" ");
            }
            System.out.println();
        }*/
        
        return nameSpans;
	}

	public static String[] OpenNLPTokenize( String strToTokenize ) throws IOException {
		//Loading the Tokenizer model 
		InputStream inputStream = new FileInputStream("resource/en-token.bin"); 
		TokenizerModel tokenModel = new TokenizerModel(inputStream); 

		//Instantiating the TokenizerME class 
		TokenizerME tokenizer = new TokenizerME(tokenModel); 

		/*Pattern p = Pattern.compile("-$(.*)\\s+", Pattern.MULTILINE);
		Matcher m = p.matcher(strToTokenize);
		if ( m.find()) {
			System.out.println(m.group(1));
		}*/
		
		//strToTokenize = strToTokenize.replaceAll("-[\\n\\r](.*)\\s+", "$1\\n");

		//Tokenize the given raw text 
		String tokens[] = tokenizer.tokenize(strToTokenize);  
		
		return tokens;
	}
	
	

	private static int countWords(String text) {
		String[] words = text.split("\\s+");
		
		return words.length;
	}


	
	public static String[] detectSentence(String strToDetect) throws IOException {
		
		//SentenceModel sentenceModel = train(generateTrainText());
		
		//Loading sentence detector model 
		InputStream inputStream = new FileInputStream("resource/en-sent.bin");
		SentenceModel sentenceModel = new SentenceModel(inputStream); 

		SentenceDetector sentenceDetector = new SentenceDetectorME(sentenceModel);
		String[] sentences = sentenceDetector.sentDetect(strToDetect);

/*		System.out.println("Detected sentences (" + sentences.length + "):");
		for (String sentence : sentences) {
			System.out.println(sentence);
		}
		*/
		return sentences;
		
	}
	
	private static SentenceModel train(final String trainText) throws IOException {
		try (ObjectStream<String> lineStream = new PlainTextByLineStream(
				() -> new ByteArrayInputStream(trainText.getBytes()), Charset.forName("UTF-8"));
				ObjectStream<SentenceSample> sampleStream = new SentenceSampleStream(lineStream)) {
			SentenceDetectorFactory sdFactory = new SentenceDetectorFactory("en", true, null, null);
			return SentenceDetectorME.train("en", sampleStream, sdFactory, TrainingParameters.defaultParams());
		}
	}
	
	private static String generateTrainText() {
		final String lineSeparator = System.lineSeparator();
		StringBuilder sb = new StringBuilder();
		for (String space : Arrays.asList(" ", "\t")) {
			for (String end : Arrays.asList(".", "!", "?", "...")) {
				for (String trainSentence : Arrays.asList("Train sentence", "This is a demo sentence", "Demo sentence")) {
					sb.append(trainSentence).append(end);
					sb.append(lineSeparator);
					sb.append(space).append(trainSentence).append(end);
					sb.append(lineSeparator);
					sb.append(space).append(trainSentence).append(end).append(space);
					sb.append(lineSeparator);
				}
			}
		}
		return sb.toString();
	}

	public static boolean isNullOrEmpty(String item) {
		if(item != null){
			item = item.trim();
		}

		if ( item == null)	{            
			return true;
		}
		else if (item.length() == 0) {
			return true;
		}
		else {       
			return false;
		}
		
	}


	public static Document parseXML(File file) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

		
		factory.setValidating(false);
		factory.setNamespaceAware(true);
		factory.setFeature("http://xml.org/sax/features/namespaces", false);
		factory.setFeature("http://xml.org/sax/features/validation", false);
		factory.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		
		DocumentBuilder builder = factory.newDocumentBuilder();
		
		Document document = builder.parse( file );
		document.getDocumentElement().normalize();
		 
		return document;
		
		
	}


	public static NodeList findNodes(Document doc, String xpath_expression) throws XPathExpressionException {
		
		 XPath xPath =  XPathFactory.newInstance().newXPath();
	        
         NodeList nodeList = (NodeList) xPath.compile(xpath_expression).evaluate( doc, XPathConstants.NODESET);
         
         return nodeList;
         
	}


	public static String convertNodeToText(NodeList nodes) {
		String text = "";
		
		for(int i = 0; i< nodes.getLength(); i++) {
			Node node = nodes.item(i);
			
			try {
				//text += nodeToString( node );
				text += elementToString(node );
			} catch (TransformerFactoryConfigurationError e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
		
		return text;
	}


	private static String nodeToString(Node node) throws TransformerFactoryConfigurationError, TransformerException {

		StringWriter sw = new StringWriter();
		
		Transformer t = TransformerFactory.newInstance().newTransformer();
		t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		t.setOutputProperty(OutputKeys.INDENT, "yes");
		t.transform(new DOMSource(node), new StreamResult(sw));
		
		return sw.toString();

	}


	public static String getFullTextFromXMLFile(String xmlFileWithPath) throws IOException {
		
		Path path = FileSystems.getDefault().getPath( xmlFileWithPath);
		String fileContent = "";
		try {
			fileContent = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
		} catch (IOException e) {
			//e.printStackTrace();
			throw e;
		}
		
		//convert
		fileContent = fileContent.replaceAll("<[^>]+>", "");
		
		fileContent = TDMUtil.dehypendateText( fileContent );
		
		//System.out.println(newContent);
		
		return fileContent;
		
	}

	/**
	 * Remove xml tags.
	 * @param stringWithMarkup
	 * @return
	 */
	public static String stripXMLTags(String stringWithMarkup) {
		
		String newContent = stringWithMarkup.replaceAll("<[^>]+>", "");
		return newContent;
	}
	
	/**
	 * Recursively renames the namespace of a node.
	 * @param node the starting node.
	 * @param namespace the new namespace. Supplying <tt>null</tt> removes the namespace.
	 */
	public static void renameNamespaceRecursive(Node node, String namespace) {
		System.out.println("node name: " + node.getNodeName() + "\tnode namespace: " + node.getNamespaceURI() + "\tnode value: " + node.getNodeValue());
	    Document document = node.getOwnerDocument();
	    if (node.getNodeType() == Node.ELEMENT_NODE) {
	        document.renameNode(node, namespace, node.getNodeName());
	    }
	    NodeList list = node.getChildNodes();
	    for (int i = 0; i < list.getLength(); ++i) {
	        renameNamespaceRecursive(list.item(i), namespace);
	    }
	}
	
	/**
	 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
	 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
	 * @version <tt>$Revision$</tt>
	 * $Id$
	 */
	public static String elementToString(Node n) {

	    String name = n.getNodeName();

	    short type = n.getNodeType();
	    
	    if ( name.equalsIgnoreCase("xref")) {	//remove xref
	    	return "";
	    }
	    if ( name.equalsIgnoreCase("italic") || name.equalsIgnoreCase("sub") || name.equalsIgnoreCase("sup")) {
	    	return n.getTextContent();
	    }

	    if (Node.CDATA_SECTION_NODE == type) {
	      return "<![CDATA[" + n.getNodeValue() + "]]&gt;";
	    }

	    if (name.startsWith("#")) {
	      //return "";
	    	return n.getNodeValue();
	    }

	    StringBuffer sb = new StringBuffer();
	    sb.append('<').append(name);

	    NamedNodeMap attrs = n.getAttributes();
	    if (attrs != null) {
	      for (int i = 0; i < attrs.getLength(); i++) {
	        Node attr = attrs.item(i);
	        sb.append(' ').append(attr.getNodeName()).append("=\"").append(attr.getNodeValue()).append(
	            "\"");
	      }
	    }

	    String textContent = null;
	    NodeList children = n.getChildNodes();

	    if (children.getLength() == 0) {
	      if ((textContent = n.getTextContent()) != null && !"".equals(textContent)) {
	        sb.append(textContent).append("</").append(name).append('>');
	        ;
	      } else {
	        sb.append("/>").append('\n');
	      }
	    } else {
	      sb.append('>').append('\n');
	      boolean hasValidChildren = false;
	      for (int i = 0; i < children.getLength(); i++) {
	        String childToString = elementToString(children.item(i));
	        if (!"".equals(childToString)) {
	          sb.append(childToString);
	          hasValidChildren = true;
	        }
	      }

	      if (!hasValidChildren && ((textContent = n.getTextContent()) != null)) {
	        sb.append(textContent);
	      }

	      sb.append("</").append(name).append('>');
	    }

	    return sb.toString();
	  }
	
	/*public static String getTextContent(final Node n)
	   {
	      if (n.hasChildNodes())
	      {
	         StringBuffer sb = new StringBuffer();
	         NodeList nl = n.getChildNodes();
	         for (int i = 0; i < nl.getLength(); i++)
	         {
	            sb.append(TDMUtil.elementToString(nl.item(i)));
	            if (i < nl.getLength() - 1)
	            {
	               sb.append('\n');
	            }
	         }

	         String s = sb.toString();
	         if (s.length() != 0)
	         {
	            return s;
	         }
	      }

	      Method[] methods = Node.class.getMethods();

	      for (Method getTextContext : methods)
	      {
	         if ("getTextContent".equals(getTextContext.getName()))
	         {
	            try
	            {
	               return (String)getTextContext.invoke(n, TDMUtil.EMPTY_ARRAY);
	            }
	            catch (Exception e)
	            {
	               System.out.println("Failed to invoke getTextContent() on node " + e.getMessage());
	               return null;
	            }
	         }
	      }

	      String textContent = null;

	      if (n.hasChildNodes())
	      {
	         NodeList nl = n.getChildNodes();
	         for (int i = 0; i < nl.getLength(); i++)
	         {
	            Node c = nl.item(i);
	            if (c.getNodeType() == Node.TEXT_NODE)
	            {
	               textContent = n.getNodeValue();
	               if (textContent == null)
	               {
	                  // TODO This is a hack. Get rid of it and implement this properly
	                  String s = c.toString();
	                  int idx = s.indexOf("#text:");
	                  if (idx != -1)
	                  {
	                     textContent = s.substring(idx + 6).trim();
	                     if (textContent.endsWith("]"))
	                     {
	                        textContent = textContent.substring(0, textContent.length() - 1);
	                     }
	                  }
	               }
	               if (textContent == null)
	               {
	                  break;
	               }
	            }
	         }

	         // TODO This is a hack. Get rid of it and implement this properly
	         String s = n.toString();
	         int i = s.indexOf('>');
	         int i2 = s.indexOf("</");
	         if (i != -1 && i2 != -1)
	         {
	            textContent = s.substring(i + 1, i2);
	         }
	      }

	      return textContent;
	}
	*/


	public static boolean isOnPersonNameStopList(String personName) {
		boolean result = false;
		
		if ( PERSON_STOP_SET.contains(personName ) ) {
			result = true;
		}
		
		return result;
	}


	public static boolean isOnPlaceNameStopList(String placeName) {
		// TODO Auto-generated method stub
		return false;
	}




	public static Set<String> findISSNForContentSet(Connection conn, String contentSetName) throws Exception {
		
		Set<String> issns = new HashSet<>();
		
		String query = "select  identifier_value from cmi_agreement_cs_identifiers where content_set_name='" + contentSetName + "' and identifier_type='issn'";
		
		try ( Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			while( rs.next() ) {
				String issn = rs.getString("identifier_value");
				issns.add(issn);
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		return issns;
	}


	public static String getTitleForContentSet(Connection conn, String contentSetName) throws Exception {
		String title = null;
		
		String query = "select  title_name from cmi_agreement_content_sets where content_set_name='" + contentSetName + "'";
		
		try ( Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			if( rs.next() ) {
				title = rs.getString("title_name");
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		return title;
	}

	/**
	 * Get non-digit pages, and digital page ranges
	 * @param pages
	 * @return
	 */
	public static String findPageRange(List<String> pages) {
		String page_range = "";
		
		int digit_min = 0;
		int digit_max = 0;
		
		for(String page: pages ) {
			if ( page.matches("^\\d+$")) {
				int value = new Integer( page ).intValue();
				if ( digit_min == 0 ) {
					digit_min = value;
				}
				else if ( value < digit_min ) {
					digit_min = value;
				}
				
				if ( digit_max == 0 ) {
					digit_max = value;
				}
				else if ( value > digit_max ) {
					digit_max = value;
				}
			}
			else {
				page_range += page + ", ";
			}
		}
		
		if ( digit_min != 0 && digit_max != 0 ) {
			page_range += "page " + digit_min + "-" + digit_max;
		}
		
		
		return page_range;
	}

	
	
	/**
	 * Use au_id find active SU file name by querying archive DB.
	 * @param conn
	 * @param auid
	 * @return
	 * @throws Exception 
	 */
	public static String findActiveSUOfAU(Connection conn, String auid) throws Exception {
		String activeSUXMLFileName = null;
		
		String cu_query = "select pmd_object_id, a_ingest_timestamp from a_cu where pmd_au_id='" + auid + "' and a_ingest_timestamp in "
						+ " (select max(a_ingest_timestamp) from a_cu where pmd_au_id='" + auid + "' and pmd_status='Active' ) ";
		
		
		String cu_id = null;
		String su_id = null;
		try ( Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(cu_query);

			if( rs.next() ) {
				cu_id = rs.getString("pmd_object_id");
			}
			
			if ( cu_id != null ) {
				//logger.info( "\tFound most recent active CU " + cu_id );
				String su_query = "select pmd_object_id from a_su where a_au_id='" + auid + "' and a_cu_id='" + cu_id + "' and pmd_status='Active' and "
									//+ " (a_content_function='Text: Marked Up Header' or "
									+ " pmd_mime_type='application/xml' and a_content_function='Text: Marked Up Full Text' ";
				
				rs = stmt.executeQuery(su_query);
				
				while ( rs.next()) {
					su_id = rs.getString("pmd_object_id");
					//logger.info( "\tFound active SU " + su_id );
				}
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		if ( su_id != null ) {
			activeSUXMLFileName = su_id.substring( su_id.lastIndexOf("/") + 1) + ".xml";
			//logger.info( "\tFound active SU file " + activeSUXMLFileName);
		}
		else {
			//logger.info("\tNo full text SU for " + auid );
		}
		
		return activeSUXMLFileName;
	}
	
	
	/**
	 * Use au_id find active xml header SU file name by querying archive DB.
	 * Find a SU: pmd_status='Active', a_content_function='Text: Marked Up Header', pmd_mime_type='application/xml'. If not found, throw Exception.
	 * @param conn
	 * @param auid
	 * @return
	 * @throws Exception 
	 */
	public static String findActiveXMLHeaderSUOfAU(String auid) throws Exception {
		String activeSUXMLFileName = null;
		
		String cu_query = "select pmd_object_id, a_ingest_timestamp from a_cu where pmd_au_id='" + auid + "' and a_ingest_timestamp in "
						+ " (select max(a_ingest_timestamp) from a_cu where pmd_au_id='" + auid + "' and pmd_status='Active' ) ";
		
		
		String cu_id = null;
		String su_id = null;
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(cu_query);

			if( rs.next() ) {
				cu_id = rs.getString("pmd_object_id");
			}
			
			if ( cu_id != null ) {
				//logger.info( "\tFound most recent active CU " + cu_id );
				String su_query = "select pmd_object_id from a_su where a_au_id='" + auid + "' and a_cu_id='" + cu_id + "' and pmd_status='Active' and "
									+ " pmd_mime_type='application/xml' and a_content_function='Text: Marked Up Header' ";
				
				rs = stmt.executeQuery(su_query);
				
				while ( rs.next()) {
					su_id = rs.getString("pmd_object_id");
					//logger.info( "\tFound active SU " + su_id );
				}
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		if ( su_id != null ) {
			activeSUXMLFileName = su_id.substring( su_id.lastIndexOf("/") + 1) + ".xml";
			//logger.info( "\tFound active SU file " + activeSUXMLFileName);
		}
		else {
			throw new Exception("No active XML SU header file");
		}
		
		return activeSUXMLFileName;
	}


	
	
	/**
	 * Use au_id find active xml SU file name by querying archive DB.
	 * Find a SU: pmd_status='Active', a_content_function='Text: Marked Up Full Text', pmd_mime_type='application/xml'. If not found, throw Exception.
	 * @param conn
	 * @param auid
	 * @return
	 * @throws Exception 
	 */
	public static String findActiveXMLSUOfAU(String auid) throws Exception {
		String activeSUXMLFileName = null;
		
		String cu_query = "select pmd_object_id, a_ingest_timestamp from a_cu where pmd_au_id='" + auid + "' and a_ingest_timestamp in "
						+ " (select max(a_ingest_timestamp) from a_cu where pmd_au_id='" + auid + "' and pmd_status='Active' ) ";
		
		
		String cu_id = null;
		String su_id = null;
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(cu_query);

			if( rs.next() ) {
				cu_id = rs.getString("pmd_object_id");
			}
			
			if ( cu_id != null ) {
				//logger.info( "\tFound most recent active CU " + cu_id );
				String su_query = "select pmd_object_id from a_su where a_au_id='" + auid + "' and a_cu_id='" + cu_id + "' and pmd_status='Active' and "
									//+ " (a_content_function='Text: Marked Up Header' or "
									+ " pmd_mime_type='application/xml' and a_content_function='Text: Marked Up Full Text' ";
				
				rs = stmt.executeQuery(su_query);
				
				while ( rs.next()) {
					su_id = rs.getString("pmd_object_id");
					//logger.info( "\tFound active SU " + su_id );
				}
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		if ( su_id != null ) {
			activeSUXMLFileName = su_id.substring( su_id.lastIndexOf("/") + 1) + ".xml";
			//logger.info( "\tFound active SU file " + activeSUXMLFileName);
		}
		else {
			throw new Exception("No active XML SU file");
		}
		
		return activeSUXMLFileName;
	}
	
	

	/**
	 * Find active html files from SU: pmd_status='Active', pmd_mime_type='text/html' and a_content_function='Rendition: Web'
	 * @param auid
	 * @return
	 * @throws Exception 
	 */
	public static List<String> findActiveHTMLOfAU(String auid) throws Exception {
		List<String> activeSUHTMLFileNames = new ArrayList<>();
		
		String cu_query = "select pmd_object_id, a_ingest_timestamp from a_cu where pmd_au_id='" + auid + "' and a_ingest_timestamp in "
						+ " (select max(a_ingest_timestamp) from a_cu where pmd_au_id='" + auid + "' and pmd_status='Active' ) ";
		
		
		String cu_id = null;
		String su_id = null;
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(cu_query);

			if( rs.next() ) {
				cu_id = rs.getString("pmd_object_id");
			}
			
			if ( cu_id != null ) {
				//logger.info( "\tFound most recent active CU " + cu_id );
				String su_query = "select pmd_archive_file_name from a_su where a_au_id='" + auid + "' and a_cu_id='" + cu_id + "' and pmd_status='Active' and "
									+ " pmd_mime_type='text/html' and a_content_function='Rendition: Web' ";
				
				rs = stmt.executeQuery(su_query);
				
				while ( rs.next()) {
					String filename = rs.getString("pmd_archive_file_name");
					activeSUHTMLFileNames.add(filename);
					//logger.info( "\tFound active SU " + su_id );
				}
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		

		if (activeSUHTMLFileNames == null || activeSUHTMLFileNames.isEmpty())  {
			throw new Exception("No active SU HTML file");
		}
		
		return activeSUHTMLFileNames;
	}




	/**
	 * Not in use since there might be multiple pmd files.
	 * Use pmd file finds active SU file name.
	 * @param csname
	 * @param auid
	 * @param subDir 
	 * @return
	 * @throws Exception
	 */
	public static String findActiveSUOfAUFromPMDFile(String csname, String auid, String subDir) throws Exception {
		
		String activeXMLFileName = null;
		
		//find pmd file
		String dataDirName = "input" + File.separator;
		if ( subDir != null && !subDir.isEmpty()) {
			dataDirName += subDir + File.separator;
		}
		dataDirName += csname + File.separator + auid.substring(auid.lastIndexOf("/")+1) + File.separator + "data";
		File dataDir = new File( dataDirName  );
		File[] pmdFiles = dataDir.listFiles( new FilenameFilter() {
			public boolean accept(File dir, String name ) {
				return name.endsWith(".pmd");
				
			}
		});
		
		if ( pmdFiles.length != 1 ) {
			throw new Exception("Invalid PMD file " + dataDir.getPath() + ".pmd");
		}
		
		//parse pmd file
		Document doc;
		try {
			doc = TDMUtil.parseXML( pmdFiles[0]);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			throw new Exception( pmdFiles[0] + " cannot be parsed. " );
		}

		Element root = doc.getDocumentElement();
		//System.out.println(root.getNodeName());
		
		//select the most recent CU if there is multiple
		NodeList CU_nodes = TDMUtil.findNodes(doc,  "/PMD/ArchivalUnit/ContentUnit[@status='Active']");
		if ( CU_nodes == null ) {
			throw new Exception("Cannot find active CU");
		}
		
		NodeList SU_nodes = null;
		if ( CU_nodes.getLength() == 1 ) {
			SU_nodes = TDMUtil.findNodes( doc, "/PMD/ArchivalUnit/ContentUnit/FunctionalUnit[@contentFunction='Text: Marked Up Full Text']/StorageUnit[@status='Active'] " +
														" |  /PMD/ArchivalUnit/ContentUnit/FunctionalUnit[@contentFunction='Text: Marked Up Header']/StorageUnit[@status='Active'] ");
		}
		else {	//there are multiple CUs, select the most recent one
			String CU_id = findMostRecentCU( CU_nodes );
			
			if ( CU_id != null && ! CU_id.equals("")) {
				SU_nodes = TDMUtil.findNodes( doc, "/PMD/ArchivalUnit/ContentUnit[@id='" + CU_id + "']/FunctionalUnit[@contentFunction='Text: Marked Up Full Text']/StorageUnit[@status='Active'] " +
									" |  /PMD/ArchivalUnit/ContentUnit/FunctionalUnit[@contentFunction='Text: Marked Up Header']/StorageUnit[@status='Active'] ");
			}
			
		}
		
		//there should be only 1 active SU
		if ( SU_nodes == null || SU_nodes.getLength() == 0 ) {
			throw new Exception("Cannot find active SU");
		}
		if ( SU_nodes.getLength() != 1 ) {
			throw new Exception("Active SU count is not 1");
		}
		
		Element SU_e = (Element)SU_nodes.item(0);
		String su_id = SU_e.getAttribute("objID");		//ark:/27927/pfnm9p34f
		
		activeXMLFileName = su_id.substring( su_id.lastIndexOf("/") + 1) + ".xml";  //pfnm9p34f.xml
		logger.info( programName + "findActiveSUOfAUFromPMDFile: Found active SU file " + activeXMLFileName);
		
		return activeXMLFileName;
	}


	/**
	 * Compare "created" attribute, find the most recent CU's id.
	 * @param cU_nodes
	 * @return
	 */
	private static String findMostRecentCU(NodeList CU_nodes) {
		String id = "";
		
		if ( CU_nodes == null || CU_nodes.getLength() == 0 ) {
			return null;
		}
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");		//2011-12-16T13:51:01.035-05:00
		Date max_date = null;
		for(int i =0; i< CU_nodes.getLength(); i++ ) {
			Element CU_e = (Element) CU_nodes.item(i);
			String created_attr = CU_e.getAttribute("created");
			String CU_id = CU_e.getAttribute("id");
			try {
				Date date = formatter.parse( created_attr);
				
				if ( max_date == null ) {
					max_date = date;
				}
				else if ( date.after(max_date) ) {
					max_date = date;
					id = CU_id;
				}
				
			} catch (ParseException e) {
				logger.error( programName + ":findMostRecentCU: Cannot parse created string " + created_attr);
				continue;
			}
		
		}
		
		return id;
	}


	/**
	 * Find a SU: pmd_status='Active', a_content_function='Rendition: Page Images', pmd_mime_type='application/pdf'
	 * @param auid
	 * @return
	 * @throws Exception
	 */
	public static String findPDFFileName(String auid) throws Exception {
		String pdfname = null;
		
		String cu_query = "select pmd_object_id, a_ingest_timestamp from a_cu where pmd_au_id='" + auid + "' and a_ingest_timestamp in "
				+ " (select max(a_ingest_timestamp) from a_cu where pmd_au_id='" + auid + "') and pmd_status='Active' ";


		String cu_id = null;

		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(cu_query);

			if( rs.next() ) {
				cu_id = rs.getString("pmd_object_id");
			}

			if ( cu_id != null ) {
				//logger.info( "\tFound most recent active CU " + cu_id );
				String su_query = "select pmd_archive_file_name from a_su where a_au_id='" + auid + "' and a_cu_id='" + cu_id + "' and pmd_status='Active'"
						+ " and a_content_function='Rendition: Page Images' and pmd_mime_type='application/pdf'";  // and pmd_format_status!='Not Well Formed'

				rs = stmt.executeQuery(su_query);

				while ( rs.next()) {
					pdfname = rs.getString("pmd_archive_file_name");
					//logger.info( "\tFound PDF file " + pdfname );
				}
			}

			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		if ( pdfname == null ) {
			throw new Exception("No active PDF SU file");
		}


		return pdfname;
	}


	/**
	 * Text has ben dehyphened at end of line.
	 * @param pdfFile
	 * @return
	 * @throws IOException
	 */
	public static String getFulltextFromPDF(String pdfFile) throws IOException {
		String fulltext = "";

		PDDocument	document = null;
		try { 
			document = PDDocument.load(new File(pdfFile));     
            fulltext =  new PDFTextStripper().getText(document);
            document.close();

        } catch (IOException e ) {
			//e.printStackTrace();
			throw e; 
		}
		finally
		{
		   if( document != null )
		   {
			   document.close();
		   }
		}
		
		
		fulltext = TDMUtil.dehypendateText( fulltext );
		
		//System.out.println(fulltext);
		
		return fulltext;
	}


	public static String dehypendateText(String text) {
		
		if ( text == null ) {
			return "";
		}
		
		//remove end of line hyphens
	    text = Pattern.compile("^(.*)?-" + System.getProperty("line.separator") + "([^\\s]*)?\\s+(.*)?" + System.getProperty("line.separator") , 
	    		     Pattern.MULTILINE).matcher(text).replaceAll("$1$2"+  System.getProperty("line.separator") + "$3" +  System.getProperty("line.separator"));
	   
		return text;
	}


	public List<DWIssue> findAllIssuesOfAContentSet(String cs, DWJournal journal) throws Exception {
		List<DWIssue> archived_issues = new ArrayList<>();
		
		String query = "select * from dw_issue where issn_no='" + cs + "' and issue_type='Physical' order by all_to_number(vol_no), all_to_number(issue_no)";
		//logger.info( query );
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			while( rs.next() ) {
				
				String vol_no = rs.getString("vol_no");
				String issue_no = rs.getString("issue_no");
				int issue_id = rs.getInt("issue_id");
				int journal_id = rs.getInt("journal_id");
				int volume_id = rs.getInt("volume_id");
				String journal_title = rs.getString("journal_title");
				String pub_year = rs.getString("pub_year");
				int seq_in_volume = rs.getInt("seq_in_volume");
				int article_count = rs.getInt("article_count");
				int ic_article_count = rs.getInt("ic_article_count");
				String issue_type = rs.getString("issue_type");
				String issue_completeness = rs.getString("issue_completeness");
				String binding = rs.getString("binding");
				
				DWIssue issue = new DWIssue(cs, vol_no, issue_no);
				issue.setJournal_title(journal_title);
				issue.setPub_year(pub_year);
				issue.setSeq_in_volume(seq_in_volume);
				issue.setArticle_count(article_count);
				issue.setWebsite_article_count(ic_article_count);
				issue.setIssue_type(issue_type);
				issue.setIssue_completeness(issue_completeness);
				issue.setBinding(binding);
				
				archived_issues.add(issue);
				
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		
		return archived_issues;
	}


	/**
	 * 
	 * @param filename The file name with path. ie config/cs_list
	 * @return
	 * @throws IOException
	 */
	public static List<String> getLineContentFromFile(String filename) throws IOException {
		
		List<String> lines = new ArrayList<>();
		
		//read issn numbers from file
		String inputFile = filename;

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
			/*
			 * if ( ! line.startsWith("ISSN_")) { logger.error("Invalid ISSN number " + line
			 * + " in " + filename ); continue; }
			 */
			
			if ( line.isEmpty()) {
				continue;
			}

			lines.add(line.trim());
			System.out.println( line.trim());
			++lineCount;
		}
		readbuffer.close();
		System.out.println("Total lines in " + inputFile + " is " + lineCount );	
		
		return lines;
	}
	
	

	public List<String> getJournalListForAPublisher(Connection conn, String publisher_ID) throws Exception {
		List<String> issnNumbers = new ArrayList<>();
		
		String query = "";
		if ( publisher_ID.equals("medium") ) {
			query = "select issn_no, publisher_name from dw_journal where publisher_name in " 
					+ " (select publisher_name from (select publisher_name , count(issn_no) as count from dw_journal  group by publisher_name  )  "
					+ " where count <= 200 and count >= 100) order by publisher_name, issn_no";
		}
		else if ( publisher_ID.equals("small")) {
			query = "select issn_no, publisher_name from dw_journal where publisher_name in " 
					+ "(select publisher_name from (select publisher_name , count(issn_no) as count from dw_journal  group by publisher_name  ) "  
					+ " where count < 100) order by publisher_name, issn_no ";
		}
		else if ( publisher_ID.equals("IEEE")) {
			query = "select issn_no from dw_journal where publisher_id='IEEE' "
					+ "and journal_title not like '%Conference%' and journal_title not like '%Proceedings%' "
					+ "and journal_title not like '%Symposium%' and journal_title not like '%Annual Meeting%' order by issn_no";
		}
		else if ( publisher_ID.equals("CSP")) {
			query = "select issn_no from dw_journal where publisher_id in ( select distinct p_provider_id from p_batch where p_profile_id='JSTOR-Profile.xml') "
					+ " and publisher_id!='AGU' and publisher_id !='HAWORTH' order by publisher_id, issn_no";
		}
		else {
			
			query = "select distinct(csets.content_set_name) as ISSN_NO "+
				"from cmi_agreement_content_sets csets " +
				"where content_type='E-Journal Content' and provider_id='" + publisher_ID + "' " +
				" order by ISSN_NO";
		}
		
	
		try ( 	Statement sstmt = conn.createStatement()	  ) {

			ResultSet rs = sstmt.executeQuery(query);

			
			while ( rs.next() ) {
				String issn_no = rs.getString("ISSN_NO");

				issnNumbers.add( issn_no );
			}	
			
			
			if ( issnNumbers.isEmpty() ) {
				System.out.println(query);
				logger.error("DataLoaderUtil:getJournalsOfAPublisher " + publisher_ID + " is an invalid publisher_id. No journals found.");
				throw new Exception("Invalid publisher id");
			}

			rs.close();
		}
		catch(SQLException sqle) {
			logger.error( "DataLoaderUtil:getJournalsOfAPublisher:" + query + sqle.getMessage()  );
			
		}
		catch(Exception e) {
			logger.error("Other Exception in getJournalsOfAPublisher");
			throw new Exception(e);
		}
		
		logger.info( "Total distinct journal issn_no=" + issnNumbers.size());
		
		return issnNumbers;
	}
	
	

	public List<String> getArchivedJournalListForAPublisher(Connection conn, String publisher_ID) throws Exception {
		List<String> issnNumbers = new ArrayList<>();
		
		String query = "select issn_no from dw_journal where publisher_id='" + publisher_ID.toUpperCase().trim() 
				+ "' and jnl_completeness_status is not null order by issn_no ";
		
		try ( 	Statement sstmt = conn.createStatement()	  ) {

			ResultSet rs = sstmt.executeQuery(query);

			
			while ( rs.next() ) {
				String issn_no = rs.getString("ISSN_NO");

				issnNumbers.add( issn_no );
			}	
			
			
			if ( issnNumbers.isEmpty() ) {
				System.out.println(query);
				logger.error("DataLoaderUtil:getArchivedJournalListForAPublisher " + publisher_ID + " is an invalid publisher_id. No journals found.");
				throw new Exception("Invalid publisher id");
			}

			rs.close();
		}
		catch(SQLException sqle) {
			logger.error( "DataLoaderUtil:getArchivedJournalListForAPublisher:" + query + sqle.getMessage()  );
			
		}
		catch(Exception e) {
			logger.error("Other Exception in getArchivedJournalListForAPublisher");
			throw new Exception(e);
		}
		
		logger.info( "Total distinct journal issn_no=" + issnNumbers.size());
		
		return issnNumbers;
	}



	private static ComboPooledDataSource getPooledDataSource(String server) throws Exception {
		
        ComboPooledDataSource cpds = new ComboPooledDataSource(); 

        try {
        	if ( server.equals("DEV")) { //DEV environment
        		//cpds.setDriverClass( "org.oracle.Driver" );  //loads the jdbc driver 
        		cpds.setJdbcUrl( Jdbc_dev_Url ); 
        		cpds.setUser(Jdbc_dev_User); 
        		cpds.setPassword(Jdbc_dev_Password); 

        		// the settings below are optional -- c3p0 can work with defaults 
        		cpds.setMinPoolSize(3); 
        		cpds.setAcquireIncrement(3); 
        		cpds.setMaxPoolSize(10); 

        	}
        	else if ( server.equals("PROD")) {  //PROD environment

        		//cpds.setDriverClass( "org.oracle.Driver" );  //loads the jdbc driver 
        		cpds.setJdbcUrl( Jdbc_dw_Url ); 
        		cpds.setUser(Jdbc_dw_User); 
        		cpds.setPassword(Jdbc_dw_Password); 

        		// the settings below are optional -- c3p0 can work with defaults 
        		cpds.setMinPoolSize(30); 
        		cpds.setAcquireIncrement(5); 
        		cpds.setMaxPoolSize(50); 
        		//cpds.setDebugUnreturnedConnectionStackTraces(true); //this affect logger
        		cpds.setIdleConnectionTestPeriod(300);
        		cpds.setMaxIdleTime(900);
        		cpds.setMaxConnectionAge(28800);
        		cpds.setCheckoutTimeout(120000);
        		cpds.setUnreturnedConnectionTimeout(900);

        	}
        }
        catch (Exception e) {
        	logger.catching( Level.FATAL, e );
        	throw new Exception("Error in gettting a DataSource.",e);
        }
        
        return cpds;
		
	}
	
	public static Connection getDWConnection_pooled(String env) throws Exception {
		
		if (dataSource == null ) {
			dataSource = getPooledDataSource(env);
		}
		
		Connection conn = null;

		try {
			conn = dataSource.getConnection();
			
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}

		return conn;
	}


	public static Connection getConnection(String server) throws Exception {
		
		Properties props = new Properties();
    	try {
    		props.load(TDMUtil.class.getClassLoader().getResourceAsStream("NLP.properties"));
    	} 
    	catch (IOException ioe) {
    		logger.fatal( ioe.getMessage());
    		logger.catching(Level.FATAL, ioe );
    	
    		throw new Exception("Error in loading NLP.properties file",ioe);
    	}
    	catch(IllegalArgumentException iae) {
    		logger.fatal( iae.getMessage());
    		logger.catching(Level.FATAL, iae );
        	
    		throw new Exception("Error in loading NLP.properties file",iae);
    	}
    	

		Connection connection = null;
		// Get JDBC Properties
        String jdbc_dev_Url = props.getProperty("Jdbc_dev_Url");
        String jdbc_dev_User = props.getProperty("Jdbc_dev_User");
        String jdbc_dev_Password = props.getProperty("Jdbc_dev_Password");
        
        String jdbc_dw_Url = props.getProperty("Jdbc_dw_Url");
        String jdbc_dw_User = props.getProperty("Jdbc_dw_User");
        String jdbc_dw_Password = props.getProperty("Jdbc_dw_Password");
        
		
		if ( server.equals("DEV")) { //DEV environment
			connection = DriverManager.getConnection(jdbc_dev_Url, jdbc_dev_User, jdbc_dev_Password);
		}
		else if ( server.equals("PROD")) {  //PROD environment
			connection = DriverManager.getConnection(jdbc_dw_Url, jdbc_dw_User, jdbc_dw_Password);
		}
		
		return connection;
	}

	/**
	 * This method returns deduped article in order
	 * @param conn
	 * @param contentSetName
	 * @param volNo
	 * @param issueNo
	 * @return
	 * @throws Exception 
	 */
	public static List<DWArticle> getArticlesInIssue(String contentSetName, String volNo, String issueNo) throws Exception {
		
		List<DWArticle> articles = new ArrayList<>();
		
		String query1 = "select * from dw_article where issn_no='" + contentSetName + "' and vol_no=q'$" + volNo + "$' and issue_no=q'$" + issueNo + "$' "
					+ " and (duplication_flag='N' or duplication_flag is null ) order by article_seq ";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement statement = conn.createStatement(); ) {
			ResultSet rs = statement.executeQuery(query1);
			
			while( rs.next()) {
				String au_id = rs.getString("au_id");
				
				DWArticle article = new DWArticle( au_id );
				article.populateDWArticle();
				
				articles.add(article);
			}
		}
		catch (Exception e) {
			throw new Exception("Error getting articles for " + contentSetName + " v." + volNo + " n." + issueNo + " " + e.getMessage());
		}
		
		
		
		return articles;
	}

	
	/**
	 * 
	 * @param contentSetName
	 * @param volNo
	 * @param issueNo
	 * @return 2012-11-26T00:00:00Z
	 * @throws Exception
	 */
	public static String getIssuePubDateISOStr(String contentSetName, String volNo, String issueNo) throws Exception {
		
		String pub_date = null;
		
		String query1 = "select to_char(pub_date, 'YYYY-MM-DD')||'T00:00:00Z' as datewithtime from dw_issue where issn_no='" + contentSetName + "' and vol_no=q'$" + volNo + "$' and issue_no=q'$" + issueNo + "$'";
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement statement = conn.createStatement(); ) {
			ResultSet rs = statement.executeQuery(query1);
			
			if( rs.next()) {
				//pub_date = rs.getString("pub_date");
				pub_date = rs.getString("datewithtime");
			}
		}
		catch (Exception e) {
			throw new Exception("Error getting pub_date for " + contentSetName + " v." + volNo + " n." + issueNo + " " + e.getMessage());
		}
		
		return pub_date;
	}
	
	public static Date getIssuePubDate(String contentSetName, String volNo, String issueNo) throws Exception {
		
		Date pub_date = null;
		
		String query1 = "select to_date(to_char(pub_date, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') datewithtime from dw_issue where issn_no='" + contentSetName + "' and vol_no=q'$" + volNo + "$' and issue_no=q'$" + issueNo + "$'";
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement statement = conn.createStatement(); ) {
			ResultSet rs = statement.executeQuery(query1);
			
			if( rs.next()) {
				pub_date = rs.getDate("datewithtime");
			}
		}
		catch (Exception e) {
			throw new Exception("Error getting pub_date for " + contentSetName + " v." + volNo + " n." + issueNo + " " + e.getMessage());
		}

		return pub_date;
	}

	


	/**
	 * First try to get fulltext from pdf file. If not available, get fulltext from xml file. End of line hyphens have been removed.
	 * @param csname
	 * @param au_id
	 * @param subDir A subdirectory where the Content Set AU files have been stored.
	 * @return
	 * @throws Exception
	 */
	public static String getArticleFullText(String csname, String au_id, String subDir) throws Exception {
		String articleFullText = null;
		
		//begell pdf cannot be processed by pdfbox. Disable this part. Use xml to get full text.
		//first try to get full text from pdf file
		try {
			articleFullText = TDMUtil.getArticleFullTextFromPDFFile(csname, au_id, subDir );
		} catch (Exception e1) {
			logger.info( e1.getMessage());
		}
		
		if ( articleFullText != null ) {
			return articleFullText;
		}
		else {
			logger.info( programName + ":getArticleFullText: Cannot get fulltext from PDF file" );
		}
	
		//if not successful, try to get fulltext from SU xml file
		//logger.info( programName + ":getArticleFullText: Try to get fulltext from XML file ......" );
		String activeSUXML = null;
		try {
			//activeSUXML = TDMUtil.findActiveSUOfAU( csname, au_id, subDir );
			activeSUXML = TDMUtil.findActiveXMLSUOfAU(  au_id );
		}
		catch(Exception e) {
			//e.printStackTrace();
			throw new Exception("Cannot find active SU file for AU " + au_id + " " + e.getMessage());
		}

		//get article text content from the SU file
		if ( activeSUXML == null ) {
			throw new Exception("No fulltext for AU " + au_id );
		}
		
		String xmlFileWithPath = "input" + File.separator ;
		if ( subDir != null && !subDir.isEmpty()) {
			xmlFileWithPath += subDir + File.separator;
		}
		xmlFileWithPath += csname + File.separator + au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + activeSUXML;
		
		try {
			logger.info( "\t\tGet full text for " + au_id + " from " + activeSUXML );
			articleFullText = TDMUtil.getFullTextFromXMLFile( xmlFileWithPath );
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("Cannot get full text from file " + xmlFileWithPath + " " + e.getMessage());
		}

		return articleFullText;
	}




	public static String getArticleFullTextFromPDFFile(String csname, String au_id, String subDir) throws Exception {
		//logger.info( programName + ":getArticleFullTextFromPDFFile: Try to get fulltext from PDF file ......" );
		String articleFullText = null;
		
		String pdfFile = null;
		try {
			pdfFile = TDMUtil.findPDFFileName( au_id );
		} catch (Exception e) {
			logger.error( programName + ":getArticleFullTextFromPDFFile: cannot find pdf file for " + au_id + " " + e.getMessage());
			throw new Exception("Cannot find pdf file");
		}
		
		if ( pdfFile == null ) {
			logger.info( programName + ":getArticleFullTextFromPDFFile: cannot find pdf file for " + au_id );
			throw new Exception("Cannot find pdf file");
		}

		
		String pdfFileWithPath = null;
		try {
			pdfFileWithPath = inputDir + File.separator;
			
			if ( subDir != null && !subDir.isEmpty()) {
				pdfFileWithPath += subDir + File.separator;
			}
			pdfFileWithPath += csname + File.separator + au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + pdfFile;

			articleFullText = TDMUtil.getFulltextFromPDF( pdfFileWithPath );
		} catch (IOException e) {
			logger.error( programName + ":getArticleFullTextFromPDFFile: cannot read fulltext from  pdf file " + pdfFileWithPath +  " for " + au_id );
			throw new Exception("Cannot read text from pdf file");
		}
		
		return articleFullText;
	}
	
	

	public static String getArticleFullTextFromXMLFile(String csname, String au_id, String subDir) throws Exception {
		
		//logger.info( programName + ":getArticleFullTextFromXMLFile: Try to get fulltext from XML file ......" );
		String articleFullText = null;
		String activeSUXML = null;
		try {
			//activeSUXML = TDMUtil.findActiveSUOfAU( csname, au_id, subDir );
			activeSUXML = TDMUtil.findActiveXMLSUOfAU( au_id );
		}
		catch(Exception e) {
			//e.printStackTrace();
			throw new Exception("Cannot find active SU file for AU " + au_id + " " + e.getMessage());
		}

		//get article text content from the SU file
		if ( activeSUXML == null ) {
			throw new Exception("No fulltext for AU " + au_id );
		}
		
		String xmlFileWithPath = "input" + File.separator ;
		if ( subDir != null && !subDir.isEmpty()) {
			xmlFileWithPath += subDir + File.separator;
		}
		xmlFileWithPath += csname + File.separator + au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + activeSUXML;
		
		try {
			logger.info( "\t\tGet full text for " + au_id + " from " + activeSUXML );
			articleFullText = TDMUtil.getFullTextFromXMLFile( xmlFileWithPath );
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("Cannot get full text from file " + xmlFileWithPath + " " + e.getMessage());
		}

		return articleFullText;

	}




	public List<DWIssue> findAllIssuesOfAContentSetVolume(String cs, DWJournal journal, String volume) throws Exception {
		List<DWIssue> archived_issues = new ArrayList<>();
		
		String query = "select * from dw_issue where issn_no='" + cs + "' and vol_no=q'$" + volume + "$' and issue_type='Physical' order by all_to_number(issue_no)";
		//logger.info( query );
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			while( rs.next() ) {
				
				String vol_no = rs.getString("vol_no");
				String issue_no = rs.getString("issue_no");
				int issue_id = rs.getInt("issue_id");
				int journal_id = rs.getInt("journal_id");
				int volume_id = rs.getInt("volume_id");
				String journal_title = rs.getString("journal_title");
				String pub_year = rs.getString("pub_year");
				int seq_in_volume = rs.getInt("seq_in_volume");
				int article_count = rs.getInt("article_count");
				int ic_article_count = rs.getInt("ic_article_count");
				String issue_type = rs.getString("issue_type");
				String issue_completeness = rs.getString("issue_completeness");
				String binding = rs.getString("binding");
				
				DWIssue issue = new DWIssue(cs, vol_no, issue_no);
				issue.setJournal_title(journal_title);
				issue.setPub_year(pub_year);
				issue.setSeq_in_volume(seq_in_volume);
				issue.setArticle_count(article_count);
				issue.setWebsite_article_count(ic_article_count);
				issue.setIssue_type(issue_type);
				issue.setIssue_completeness(issue_completeness);
				issue.setBinding(binding);
				
				archived_issues.add(issue);
				
			}
			
			rs.close();
		}
		catch(Exception e) {
			throw e;
		}
		
		
		return archived_issues;
	}
	
	
	



	/**
	 * Return split page texts of an article. End of line hyphens have been removed.
	 * If usePdfForFullText is true, use pdf box tool to split pdf file into pages. Otherwise use articleFullText to split into 500 words pages. 
	 * @param conn
	 * @param contentSetName
	 * @param au_id
	 * @param usePdfForFullText 
	 * @param articleFullText 
	 * @param subDir 
	 * @return
	 * @throws Exception
	 */
	public static List<String> splitArticleByPages( String contentSetName, String au_id, boolean usePdfForFullText, String articleFullText, String subDir) throws Exception {
		
		List<String> pageTextList = new ArrayList<>();
		
		//if pdf file cannot be used for fulltext
		if ( ! usePdfForFullText ) {
			if ( articleFullText == null ) {
				logger.error( programName + ":splitArticleByPages: cannot get fulltext for " + au_id );
				throw new Exception("No fulltext");
			}
			
			pageTextList = TDMUtil.manaulSplitTextToPages( contentSetName, au_id, articleFullText);
			return pageTextList;
			
		}
		
		
		//use original pdf to split text to pages
		String pdfFile = null;
		try {
			pdfFile = TDMUtil.findPDFFileName( au_id );
		} catch (Exception e) {
			if ( articleFullText != null ) {
				pageTextList = TDMUtil.manaulSplitTextToPages( contentSetName, au_id, articleFullText);
				return pageTextList;
			}
			
			logger.error( programName + ":splitArticleByPages: cannot find pdf file for " + au_id + " " + e.getMessage());
			throw new Exception("Cannot find pdf file");
		}
		String pdfFileWithPath = null;
		try {
			pdfFileWithPath = inputDir + File.separator;
			
			if ( subDir != null && !subDir.isEmpty()) {
				pdfFileWithPath += subDir + File.separator;
			}
			
			pdfFileWithPath +=  contentSetName + File.separator + au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + pdfFile;

		} catch (Exception e) {
			logger.error( programName + ":splitArticleByPages: cannot read fulltext from  pdf file " + pdfFileWithPath +  " for " + au_id );
			throw new Exception("Cannot read text from pdf file");
		}
		
		//split pdf file into pages using pdfbox tool
		PDDocument document = null;
		try {
			document = PDDocument.load(new File(pdfFileWithPath)); 
			Splitter splitter = new Splitter();
			try {
				List<PDDocument> splittedDocuments = splitter.split(document);
				int index = 1;
				for(PDDocument doc: splittedDocuments) {
					try {
						String text = new PDFTextStripper().getText(doc);
						//System.out.println("*********Page " + index++ + "**************");
						//System.out.println(text);

						/*Pattern regex = Pattern.compile("^(.*)?-" + System.getProperty("line.separator") + "([^\\s]*)?\\s+(.*)?" + System.getProperty("line.separator") , Pattern.MULTILINE);
						Matcher matcher = regex.matcher(text);
						while (matcher.find())
						{
							System.out.println(matcher.group(0));
							System.out.println("$1: " + matcher.group(1));
							System.out.println("$2: " + matcher.group(2));
							System.out.println("$3: " + matcher.group(3));

						}*/

						//remove end of line hyphens
						text = TDMUtil.dehypendateText(text);

						//text = Pattern.compile("^(.*)?-" + System.getProperty("line.separator") + "([^\\s]*)?\\s+(.*)?" + System.getProperty("line.separator") , Pattern.MULTILINE).matcher(text).replaceAll("$1$2"+  System.getProperty("line.separator") + "$3" +  System.getProperty("line.separator"));
						//System.out.println(text);

						pageTextList.add(text);
					} catch (IOException e3) {
						// TODO Auto-generated catch block
						e3.printStackTrace();

					}


				}

			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
			}
			finally {
				document.close();
			}

		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		finally
		{
		   if( document != null )
		   {
			   document.close();
		   }
		}

		
		return pageTextList;
	}
	
	

	/**
	 * Not in use. 
	 * If individual chapter has its own pdf file, split it by pages. If chapter got from bookmarks, return alreay split pages in Chapter.pageTexts.
	 * @param providerId
	 * @param chapter
	 * @param chapterFullText
	 * @param inputDir "input/ebook/eup"
	 * @return
	 * @throws IOException 
	 */
/*	public static List<String> splitChapterByPages(String providerId, Chapter chapter, String chapterFullText, String inputDir) throws IOException {
		
		Book book = chapter.getParentBook();
		String bookAuId = book.getBookAuId();
		String bookcsname = book.getContentSetName();
		
		List<String> pageTextList = new ArrayList<>();
		
		//use original pdf to split text to pages
		String pdfFileName = chapter.getPdfFilename_archive();
		String pdfFileWithPath = null;
		
		if ( pdfFileName == null ) {	//don't have individual chapter pdf file
			if ( chapter.getPageTexts()!= null && !chapter.getPageTexts().isEmpty()) {	//pages have already been split when dividing bookmark chapters
				pageTextList = chapter.getPageTexts();
				return pageTextList;
			}
			else if ( chapterFullText != null ) {
				pageTextList = TDMUtil.manaulSplitTextToPages( bookcsname, bookAuId, chapterFullText);	//split by 500 words (may not be used)
				return pageTextList;
			}
			else {
				return null;
			}
		}
		
		//going to split pdf file into pages using pdfbox tool
		pdfFileWithPath = inputDir + File.separator + bookAuId.substring(bookAuId.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + pdfFileName;
		PDDocument	document = null;
		try { 
			document = PDDocument.load(new File(pdfFileWithPath));

			Splitter splitter = new Splitter();
			try {
				List<PDDocument> splittedDocuments = splitter.split(document);
				for(PDDocument doc: splittedDocuments) {
					try {
						String text = new PDFTextStripper().getText(doc);
						//System.out.println("*********Page " + index++ + "**************");
						//System.out.println(text);

						//remove end of line hyphens
						text = TDMUtil.dehypendateText(text);

						pageTextList.add(text);

					} catch (IOException e3) {
						// TODO Auto-generated catch block
						e3.printStackTrace();

					}
					finally {
						if( doc != null )
						{
							doc.close();
						}
					}

				}

			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
			}
			
			document.close();
			
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		finally
		{
		   if( document != null )
		   {
			   document.close();
		   }
		}
		return pageTextList;
		
	}

*/


	/**
	 * This method split articleText into pages. Each page is about 500 words.
	 * @param contentSetName
	 * @param au_id
	 * @param articleText
	 * @return
	 */
	public static List<String> manaulSplitTextToPages(String contentSetName, String au_id, String articleText) {
		
		/*String text = "Mary had a little lamb";

		Pattern pattern = Pattern.compile("(\\b[^\\s]*\\s+\\b){2}");
		Matcher matcher = pattern.matcher(text);

		while(matcher.find()){
		    System.out.println("Found match at: "  + matcher.group() + matcher.start() + " to " + matcher.end());
		}
		*/
		if ( articleText == null ) {
			return null;
		}
		
		//First dephyphen text
		articleText = TDMUtil.dehypendateText(articleText);

		List<String> pageTextList = new ArrayList<>();
		List<String> textLines = new ArrayList<>();

		Pattern regex = Pattern.compile("^.*?[\\n\\r]", Pattern.MULTILINE);
		Matcher matcher = regex.matcher(articleText);
		while (matcher.find())
		{
			textLines.add(matcher.group());
		}
		
		int wordCount = 0;
		String onePageText = "";
		for(String oneLineText: textLines){ 
			int wordCountInLine = TDMUtil.countWords(oneLineText);
			wordCount += wordCountInLine;
			onePageText += oneLineText  + "\\n";
			
			if ( wordCount > 500 ) {
				wordCount = 0;
				pageTextList.add(onePageText);
				onePageText = "";
			}
		}
		//the rest
		if ( onePageText != null && !onePageText.isEmpty()) {
			onePageText = TDMUtil.normalizeUTF8String(onePageText);
			pageTextList.add(onePageText);
		}

		return pageTextList;
	}


	public static String getCurrentISODate() {
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
		df.setTimeZone(tz);
		String nowAsISO = df.format(new Date());
		return nowAsISO;
	}


	/**
	 * Not in use. Creators are read from xml files.
	 * @param article_creator like MiÅ‚osÅ‚awa Barkowska,Barbara Pinowska,Jan Pinowski,Jerzy Romanowski,Kyu-Hwang Hahm
	 * @return List of Person
	 */
	/*
	public static List<Person> convertPMDCreatorsToPersonList(String article_creator, String au_id) {
		
		List<Person> creatorList = new ArrayList<>();
		
		if ( article_creator != null ) {
			
			List<String> nameList = TDMUtil.convertPMDCreatorsToNameList( article_creator );
			
			for(String name: nameList) {
				//System.out.println(name);
				Person p = new Person(name, au_id);
				
				creatorList.add(p);
			}
			
		}
		
		return creatorList;
	}
	
	*/
	
	/**
	 * 
	 * @param article_creator like MiÅ‚osÅ‚awa Barkowska,Barbara Pinowska,Jan Pinowski,Jerzy Romanowski,Kyu-Hwang Hahm
	 * @return List of person names
	 */
	public static List<String> convertPMDCreatorsToNameList(String article_creator) {
		//article_creator="MiÅ‚osÅ‚awa Barkowska,Barbara Pinowska,Jan Pinowski,Jerzy Romanowski,Kyu-Hwang Hahm";
		//H. Ahlbrecht;,G. Becher,J. Blecher,H.-O. Kalinowski,W. Raab,A. Mannschreck;
		//G. Bu;chi,F. Greuter,Takashi Tokoroyama
		//Ulrich Scho;llkopf,A. Lerch,W. Pitteroff
		//Simons, William B.
		//Azelastineâ€“Asthma Study Group*,*James P. Kemp, MD, San Diego, Calif.; David S. Pearlman, MD, Aurora, Colo.; David G. Tinkelman, MD, Atlanta, Ga.; John M. Weiler, MD, Iowa City, Iowa; David Bernstein, MD, Cincinnati, Ohio; William Busse, MD, Madison, Wis.; Arthur DeGraff, MD, Hartford, Conn.; Louis Diamond, PhD, Boulder, Colo.; Robert Dockhorn, MD, Prairie Village, Kan.; Lyndon Mansfield, MD, El Paso, Texas; William Pierson, MD, Seattle, Wash.; James Seltzer, MD, San Diego, Calif.; Sheldon Spector, MD, Los Angeles, Calif.; Marc Goldstein, MD, Philadelphia, Pa.; Michael Lawrence, MD, Taunton, Mass.; and Thomas A. Dâ€™Eletto, MD, Princeton, N.J.
		List<String> creatorList = new ArrayList<>();
		Set<String> creatorSet = new HashSet<>();
		
		if ( article_creator != null ) {
			
			//some cleanning
			article_creator.replace(";,", ","); 	//H. Ahlbrecht;,G. Becher,J. Blecher,H.-O. Kalinowski,W. Raab,A. Mannschreck;
			article_creator.replaceAll(";$", "");   //H. Ahlbrecht;,G. Becher,J. Blecher,H.-O. Kalinowski,W. Raab,A. Mannschreck;
			article_creator.replace(" MD,", "");	//AAEM Quality Assurance Committee: Developed by the AAEM Quality Assurance Committee (1997â€“2000): primary authors: Faye Y. Chiouâ€�Tan, MD, Richard W. Tim, MD, and James M. Gilchrist, MD; chairs: Cheryl F. Weber, MD (1998), John R. Wilson, MD (1999â€�2000); committee members: Timothy J. Benstead, MD, FRCP(C), Arlene M. Braker, MD, James B. Caress, MD, Sudhansu Chokroverty, MD, FRCP, Earl R. Hackett, MD, Robert L. Harmon, MD, MS, Bernadette A. Hughes, MD, Milind J. Kothari, DO, Tim Lachman, MD, Richard I. Malamut, MD, Christina M. Marciniak, MD, Robert G. Miller, MD, Kevin R. Nelson, MD, Richard K. Olney, MD, Atul T. Patel, MD, Caroline A. Quartly, MD, FRCP(C), and Karen S. Ryan, MD
			article_creator.replace(" and ", "");
			article_creator.replace(" PhD;", "");  	//I.S.J. Merkies,P.I.M. Schmitz,J.P.A. Samijn,F.G.A. Van Der MechÃ©,K.V. Toyka,P.A. van Doorn,the INFLAMMATORY NEUROPATHY CAUSE AND TREATMENT (INCAT) GROUP: Members of the INCAT group: J. Aubry, PhD; N. Baumann, PhD; P. Bouche, MD, PhD; G. Comi, MD, PhD; M.C. Dalakas, MD, PhD; H.P. Hartung, MD, PhD; R.A.C. Hughes, MD, PhD; I. Illa, MD, PhD; M.R.J. Knapp, PhD; J.â€�M. LÃ©ger, MD, PhD; R. Nemni, MD, PhD; E. Nobileâ€�Orazio, MD, PhD; K.V. Toyka, MD, PhD; P. van de Bergh, MD, PhD; F.G.A. van der MechÃ©, MD, PhD; P.A. van Doorn, MD, PhD; H.J. Willison, MD, PhD; and B. Younesâ€�Chennoufi, PhD.
			article_creator.replace(" PhD;", ""); 
			article_creator.replace("nbsp;", "");   //R. W. Birdsall,nbsp; Derris
			article_creator.replace("o;", "o");     //Gyo;zo; Garab,Jack Farineau
			
			String delimitor = ",";
			if ( article_creator.indexOf(",") == -1 && article_creator.indexOf(";") != -1 ) {
				delimitor = ";";
			}
			
			String[] creators = article_creator.split(delimitor);
			
			boolean notFullNameSplit = false;
			for(int i = 0; i< creators.length; i++) {
				if ( creators[i].indexOf(" ") == -1 ) { //only a single word without space
					notFullNameSplit = true;
				}
			}
			
			if ( notFullNameSplit ) {
				if ( creators.length == 2 ) {   //Simons, William B.
					creatorSet.add( creators[1] + " " + creators[0]); 	//change the order of "lastname, firstname" to "firstname lastname"
				}
				else {
					creatorSet.add( article_creator);  
				}
			}
			else {
				creatorSet.addAll( Arrays.asList(creators) );
			}
			
			for(String name: creatorSet) {
				creatorList.add(name);
			}
			
		}
		
		return creatorList;
	}



	/**
	 * Format value to XXXX-XXXX format.
	 * @param value
	 * @return
	 * @throws Exception 
	 */
	public static String formatISSN(String value) throws Exception {
		String formattedIssn = value.toUpperCase().trim();
		if ( formattedIssn.length() == 8 ) {
			formattedIssn = formattedIssn.substring(0, 4) + "-" + formattedIssn.substring(4);
		}
		else if ( ! formattedIssn.substring(4, 5).equals("-")) {
			throw new Exception("Invalid ISSN value " + value );
		}
		
		return formattedIssn;
	}
	
	
	/**
	 * Format isbnStr to isbn13
	 * @param isbnStr
	 * @return null if not successful
	 */
	public static String formatISBN(String isbnStr) {
		String formattedIsbn13;
		
		//These are all valid ISBN numbers: 
		//ISBN 978-0-596-52068-7
		//ISBN-13: 978-0-596-52068-7
	    //978 0 596 52068 7
	    //9780596520687
	    //ISBN-10 0-596-52068-9
	    //0-596-52068-9
		
		//null if length > 10 (we don't convert from 13 to 10), or throw Exception if invalid (length <= 10) 
		isbnStr = TDMUtil.normalizeISBN(isbnStr);
		
		if ( isbnStr.startsWith("978") || isbnStr.startsWith("979")) {	//might be isbn13
			formattedIsbn13 = TDMUtil.retrieveISBN13( isbnStr );
			
		}
		else {		//might be isbn10
			String isbn10 = null;
			try {
				isbn10 = TDMUtil.retrieveISBN10( isbnStr );
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			formattedIsbn13 = TDMUtil.convertISBN10ToISBN13( isbn10 );
		}

		
		return formattedIsbn13;
	}



	/**
	 * 
	 * @param isbn_10 is an ISBN 10 String
	 * @return null if no ISBN13 can be converted
	 */
	private static String convertISBN10ToISBN13(String isbn_10) {
		String isbn_13 = null;

		if ( isbn_10 != null && isbn_10.length() == 10 ) {
	
			isbn_13 = "978" + isbn_10.substring(0,9);

			int d;
			int sum = 0;
			for (int i = 0; i < isbn_13.length(); i++) {
				d = ((i % 2 == 0) ? 1 : 3);
				sum += ((((int) isbn_13.charAt(i)) - 48) * d);
			}
			sum = 10 - (sum % 10);
			if ( sum == 10 ) {
				sum = 0;
			}

			isbn_13 += sum;
	    
		}

		return isbn_13;
	}


	private static String retrieveISBN13(String isbnStr) {
		String isbn_13 = null;
		
		if ( isbnStr != null ) {
			isbnStr = normalizeISBN(isbnStr);
		}
		
		if ( !isBlank(isbnStr) && isbnStr.length() <= 10 ) {
			try {
				isbnStr = retrieveISBN10(isbnStr);
			} catch (Exception e) {
				logger.error( programName + ":retrieveISBN13 Error retrieving ISBN 10 from " + isbnStr );
				return null;
			}
			isbn_13 = convertISBN10ToISBN13(isbnStr);
		}
		
		Pattern p_strict = Pattern.compile("^(?:ISBN(?:-13)?:? )?"     // Optional ISBN/ISBN-13 identifier.
				+ "(?="                      // Basic format pre-checks (lookahead):
				+ "[0-9]{13}\\D*.*"          //  Require 13 digits (no separators).
				+ "|"                        // Or:
				+ "(?=(?:[0-9]+[- ]){4})"   //   Require 4 separators
				+ "[- 0-9]{17}\\D*.*"       //    out of 17 characters total.
				+ ")"                        // End format pre-checks.
				+ "(97[89][- ]?"              // ISBN-13 prefix.
				+ "[0-9]{1,5}[- ]?"         // 1-5 digit group identifier.
				+ "[0-9]+[- ]?[0-9]+[- ]?"  // Publisher and title identifiers.
				+ "[0-9])"                    // Check digit.
				+ "\\D*.*");
		Matcher m = p_strict.matcher(isbnStr);
		
		if ( m.find()) {
			isbnStr = m.group(1);
			isbnStr = isbnStr.replaceAll("[^0-9]*", "").trim();
			if ( isbnStr.length() == 13 ) {
				isbn_13 = isbnStr;
			}
		}
		else {
			
			isbnStr = normalizeISBN( isbnStr );
			
			Pattern p_relax = Pattern.compile("\\b(97[8|9]\\d{9}[0-9Xx])\\b");
			
			m = p_relax.matcher(isbnStr);
			
			if ( m.find()) {
				isbn_13 = m.group(1);
			}

		}
		
		return isbn_13;
		
	}


	/**
	 * Return null if length > 10 (we dont convert from 13 to 10), or throw Exception if invalid (length <= 10) 
	 * @param isbnStr Normalized String
	 * @return
	 * @throws Exception 
	 */
	public static String retrieveISBN10(String isbnStr) throws Exception {
		
		String isbn_10 = null;
		
		if ( !isBlank(isbnStr) && isbnStr.length() < 10 ) {
			isbnStr = String.format("%0$10s" , isbnStr).replace(' ', '0');
		}
		
		Pattern p_relax = Pattern.compile("\\b(\\d{9}[0-9Xx])\\b");
		
		Matcher m = p_relax.matcher(isbnStr);
		
		if ( m.find()) {
			isbn_10 = m.group(1);
			
			if ( !validateIsbn10( isbn_10)) {
				throw new Exception("Invalid ISBN number " + isbnStr );
			}
		}
		
		return isbn_10;

	}
	

	
	/**
	 * Checks if is blank.
	 *
	 * @param str the str
	 * @return true, if is blank
	 */
	public static boolean isBlank(String str) {
		if (str == null || str.isEmpty() || "null".equalsIgnoreCase(str)) {
			return true;
		}
		return false;
	}
	
	public static boolean validateIsbn10( String isbn )    {
        
		if ( isbn == null )  {
            return false;
        }


        //must be a 10 digit ISBN
        if ( isbn.length() != 10 ) {
            return false;
        }

        try
        {
            int tot = 0;
            for ( int i = 0; i < 9; i++ )
            {
                int digit = Integer.parseInt( isbn.substring( i, i + 1 ) );
                tot += ((10 - i) * digit);
            }

            String checksum = Integer.toString( (11 - (tot % 11)) % 11 );
            if ( "10".equals( checksum ) )
            {
                checksum = "X";
            }

            return checksum.equalsIgnoreCase( isbn.substring( 9 ) );
        }
        catch ( NumberFormatException nfe )
        {
            //to catch invalid ISBNs that have non-numeric characters in them
            return false;
        }
    }



	/**
	 * Get rid of dashes and space in the string. Remove content between parenthesis and brackets. Remove leading and ending letters. etc.
	 * @param isbnstr
	 * @return
	 */
	private static String normalizeISBN(String isbnstr) {
		
		if ( isbnstr == null ) {
			return null;
		}

		isbnstr = isbnstr.replaceAll("-", "");
		isbnstr = isbnstr.replace('\u0020', ' ');		//hyphen-minus
		isbnstr = isbnstr.replace('\u2012', ' ');		//figure dash
		isbnstr = isbnstr.replace('\u2013', ' ');		//en dash
		isbnstr = isbnstr.replace('\u2014', ' ');		//em dash
		isbnstr = isbnstr.replace('\u2015', ' ');		//horizontal bar
		isbnstr = isbnstr.replace('\u2011', ' ');		//non-breaking hyphen
		isbnstr = isbnstr.replace('\u2010', ' ');		//hyphen
		isbnstr = isbnstr.replaceAll("\\s+", "");		//remove all space
		
		isbnstr = isbnstr.toLowerCase().replaceAll("isbn.*[: ]", "").trim();
		isbnstr = isbnstr.replace(":", " ");
		isbnstr = isbnstr.replaceAll("\\(.*\\)", "");	//remove everything between parenthesis
		isbnstr = isbnstr.replaceAll("\\[.*\\]", "");
		isbnstr = isbnstr.replace("(", " ");
		isbnstr = isbnstr.replace(")", " ");
		
		isbnstr = isbnstr.replaceAll("^\\D+", "");
		isbnstr = isbnstr.replaceAll("[^0-9xX]+$", "");

		isbnstr = isbnstr.replaceAll("\\s+", "");		//remove all space
		isbnstr = isbnstr.replaceAll("^0+$", "");		//if all 0s, then to empty string(will be invalid ISBN)
		
		isbnstr = isbnstr.trim().replaceAll("^0*1$", "");		//if 1, to empty string

		return isbnstr;
	}



	/**
	 * Get one noid
	 * @param env "PROD" or "DEV"
	 * @return
	 * @throws Exception 
	 */
	public synchronized static String getNoid(String env) throws Exception {
		String noid = "";
		
		int HTTP_STATUS_OK = 200;
		int bufferSize = Integer.parseInt(TDMUtil.ldap_name_attr_noid_buffer_size);
		String noidEnvIdentifer = "";
		String url = "";
		if ( env.equalsIgnoreCase("PROD") ) {
			noidEnvIdentifer = TDMUtil.ldap_name_attr_noid_prefix_prod;
			url = TDMUtil.ldap_name_attr_noid_url_prod + "?" + bufferSize;
		}
		else {
			noidEnvIdentifer = TDMUtil.ldap_name_attr_noid_prefix_dev;
			url = TDMUtil.ldap_name_attr_noid_url_dev + "?" + bufferSize;
		}

		BufferedReader reader = null;
		int maxRetryAttemtps = 3;
		int retryCounter = 0;
		List<String> noidIds = new ArrayList<String>();
		
		do {
			retryCounter++;
			//LOGGER.info("Minting ids");
			//	LOGGER.info("Requested ids = |{}|", bufferSize);
			URL noidUrl = null;
			HttpURLConnection noidCon = null;
			try {

				noidUrl = new URL(url);
				noidCon = (HttpURLConnection) noidUrl.openConnection();
				noidCon.setDoInput(true);
				noidCon.setDoOutput(false);
				noidCon.connect();
				reader = new BufferedReader(new InputStreamReader(noidCon.getInputStream()));
				String output = null;
				if (noidCon.getResponseCode() == HTTP_STATUS_OK) {
					//logger.debug("noidEnvIdentifer={}|", noidEnvIdentifer);
					while ((output = reader.readLine()) != null) {
						if (!output.equals("")) {
							//LOGGER.info("output=|{}|", output);
							if ( output.indexOf(noidEnvIdentifer) != -1) {
								noidIds.add(output.trim().substring(output.indexOf(" ") + 1));
							} 
							else {
								logger.warn("NOID SERVICE RETURNED AN UNEXPECTED RESULT : " + output);
							}
						}
					}
				} else {
					logger.error("UNSUCCESSFUL HTTP REQUEST TO NOID SERVICE and  HTTP RETURN CODE is :{} ",
							noidCon.getResponseCode());
					throw new Exception("UNSUCCESSFUL HTTP REQUEST TO NOID SERVICE (HTTP RETURN CODE) : "
							+ noidCon.getResponseCode());
				}
			} finally {
				try {
					reader.close();
				} catch (Exception e) {
					logger.error("Exception while closing Buffered Reader", e);
				}
				try {
					noidCon.disconnect();
				} catch (Exception e) {
					logger.error("Exception while closing http connection to noid service ", e);
				}
			}
		} while (retryCounter < maxRetryAttemtps && noidIds.size() == 0);
		//logger.debug("Extracted ids = |{}|", noidIds.size());
	
		noid = (String) noidIds.remove(0);

		return noid;
	}


	public static String getPublisherWebsiteUrl(String publisher_id) throws Exception {
		String url = null;
		String query1 = "select * from cmi_providers where provider_id='" + publisher_id + "'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			
			if ( rs.next()) {
				String provider_name = rs.getString("provider_name");
				url = rs.getString("website_url");
				
			}
			else {
				throw new Exception("Cannot locate publisher with id " + publisher_id );
			}
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return url;
	}


	public static String getPubYearForAIssue(String doc_id) throws Exception {
		String pubYear = null;
		String query1 = "select pub_year from dw_issue where ark_id='" + doc_id + "'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			
			if ( rs.next()) {
				pubYear = rs.getString("pub_year");
			}
			else {
				throw new Exception("Error finding pub_year for ark_id " + doc_id );
			}
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return pubYear;
	}


	public static List<String> getListFromFile(String filename) throws IOException {
		List<String> lists = new ArrayList<>();
		
		//read issn numbers from file
		String inputFileDir = "config" ;		//under config directory
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
				System.out.println( line.trim());
				++lineCount;
			}
		}	
		readbuffer.close();
		System.out.println("Total lines in " + inputFile + " is " + lineCount );	
		
		return lists;
	}


	public static List<String> getDocIdsForAContentSet(String cs) throws Exception {
		
		List<String> idlists = new ArrayList<>();
		
		String query1 = "select ark_id from dw_issue where issn_no='" + cs.trim() + "' and ark_id is not null";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			
			String id = null;
			while ( rs.next()) {
				id = rs.getString("ark_id");
				
				if ( id != null && !id.isEmpty()) {
					idlists.add(id);
				}
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		
		return idlists;
	}


	public static List<String> getTDMContentSetsOfPublisher(String publisher) throws Exception {
		List<String> cslists = new ArrayList<>();
		
		String query1 = "select content_set_name from tdm_title where publisher_id='" + publisher.toUpperCase() + "' order by content_set_name";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			
			String cs = null;
			while ( rs.next()) {
				cs = rs.getString("content_set_name");
				
				if ( cs != null && !cs.isEmpty()) {
					cslists.add(cs);
				}
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		
		return cslists;
	}


	/**
	 * 
	 * @param cs
	 * @return
	 * @throws Exception 
	 */
	public static List<String> getIssueIDsForAContentSet(String cs) throws Exception {
		
		List<String> issueIDs = new ArrayList<>();
		
		String query1 = "select vol_no, issue_no from dw_issue where issn_no='" + cs.trim() + "' and ark_id is not null order by all_to_number(vol_no), all_to_number(issue_no)";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			
			String vol_no = null;
			String issue_no = null;
			String issue_id = null; //ISSN_07115075_v1_n1
			while ( rs.next()) {
				vol_no = rs.getString("vol_no");
				issue_no = rs.getString("issue_no");
				
				if ( vol_no != null && !vol_no.isEmpty() && issue_no != null && ! issue_no.isEmpty()) {
					issue_id = cs.trim() + "_v" + vol_no.trim() + "_n" + issue_no;
					issueIDs.add(issue_id);
				}
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		
		return issueIDs;
	}



	public static boolean checkArticlePDFUsuability(String csname, String au_id, String subDir) {
		
		boolean pdfOKFlag = true;
		
		//first try to get full text from pdf file
		String pdfFile = null;
		try {
			pdfFile = TDMUtil.findPDFFileName( au_id );
		} catch (Exception e) {
			logger.error( programName + ":checkArticlePDFUsuability: cannot find pdf file for " + au_id + " " + e.getMessage());
			pdfOKFlag = false;
		}
		
		if ( pdfFile == null ) {
			logger.info( programName + ":checkArticlePDFUsuability: cannot find pdf file for " + au_id );
			pdfOKFlag = false;
		}

		
		String pdfFileWithPath = null;
		String articleFullText = null;
		try {
			pdfFileWithPath = inputDir + File.separator;
			
			if ( subDir != null && !subDir.isEmpty()) {
				pdfFileWithPath += subDir + File.separator;
			}
			pdfFileWithPath += csname + File.separator + au_id.substring(au_id.lastIndexOf("/") + 1) + File.separator + "data" + File.separator + pdfFile;

			articleFullText = TDMUtil.getFulltextFromPDF( pdfFileWithPath );
			
			if ( articleFullText != null ) {
				if ( articleFullText.trim().isEmpty()) {
					pdfOKFlag = false;
				}
			}
			else {
				pdfOKFlag = false;
			}
			
			
		} catch (IOException e) {
			logger.error( programName + ":checkArticlePDFUsuability: cannot read fulltext from  pdf file " + pdfFileWithPath +  " for " + au_id );
			pdfOKFlag = false;
		}
		
		
	
		return pdfOKFlag;
	}


	/**
	 * For Portico TDM project book publishers, all have one content set name, except SAGE has two(SAGE KNOWLEDGE, SAGE RESEARCH METHODS). This method only returns on content set name.
	 * @param publisher
	 * @return
	 * @throws Exception
	 */
	public static String findPublisherBookContentSetName(String publisher) throws Exception {
		String csname = null;
		
		String query1 = "select content_set_name from cmi_agreement_content_sets where content_type='E-Book Content' and provider_id='" + publisher.toUpperCase().trim() + "'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				csname = rs.getString("content_set_name");
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return csname;
	}
	
	

	public static String findPublisherBookContentSetName(String publisherID, String auid) throws Exception {
		
		String csname = null;
		
		String query1 = "select a_au.pmd_content_set_name from a_au, cmi_agreement_content_sets where a_au.pmd_content_type='E-Book Content' "
						+ " and a_au.pmd_content_set_name=cmi_agreement_content_sets.content_set_name and provider_id='" + publisherID.toUpperCase().trim() + "' "
						+ " and pmd_object_id='" + auid + "'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				csname = rs.getString("pmd_content_set_name");
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return csname;
	}	
	



	public static boolean bookBelongToPublisher(String publisherBookCSName, String auid) throws Exception {
		boolean result = false;
		
		String query1 = "select count(*) from a_au where pmd_content_set_name='" + publisherBookCSName + "' and pmd_object_id='" + auid + "' and pmd_content_type='E-Book Content'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				int count = rs.getInt(1);
				if ( count == 1 ) {
					result = true;
				}
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return result;
	}


	/**
	 * Get Active xml file from A_SU table.
	 * @param auid
	 * @return Null if didn't get result
	 * @throws Exception
	 */
	public static String getBitsXmlFileNameForAU(String auid) throws Exception {
		
		String xmlFileName = null;

		String query = "select a_su.* from a_su, a_au where a_au.pmd_object_id='" + auid + "' and a_au.a_latest_active_cu_id=a_su.a_cu_id and a_su.pmd_status='Active' and a_su.pmd_mime_type='application/xml' ";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			if( rs.next() ) {

				xmlFileName = rs.getString("pmd_archive_file_name");

			}
			
			rs.close();
		}
		catch(Exception e) {
			logger.error( programName + ":getBitsXmlFileNameForAU " + e.getMessage());
			throw e;
		}
		
		return xmlFileName;
		
	}


	/**
	 * Convert month names to digit integer. January to 1, etc.
	 * @param monthStr
	 * @return
	 */
	public static int parserMonthNameToDigit(String monthStr) {
		int monthDigit = 0;
		
		if ( monthStr.matches("^\\d{1,2}$")) {	
			monthDigit = new Integer(monthStr).intValue();
			return monthDigit;
		}
		
		switch( monthStr.toLowerCase() ) {
		case "january": case "jan":
			monthDigit = 1;
			break;
		case "february": case "feb":
			monthDigit = 2;
			break;
		case "march": case "mar":
			monthDigit = 3;
			break;
		case "april": case "apr":
			monthDigit = 4;
			break;
		case "may": 
			monthDigit = 5;
			break;
		case "june": case "jue":
			monthDigit = 6;
			break;
		case "july": case "jul":
			monthDigit = 7;
			break;
		case "august": case "aug":
			monthDigit = 8;
			break;
		case "september": case "sep":
			monthDigit = 9;
			break;
		case "october": case "oct":
			monthDigit = 10;
			break;
		case "november": case "nov":
			monthDigit = 11;
			break;
		case "december": case "dec":
			monthDigit = 12;
			break;
		}
		
		return monthDigit;
	}

	
	/**
	 * 
	 * @param filename, ie fu_id	"localfile:j.ctt1g69w6r.1.pdf" 
	 * @param bookAuId
	 * @return
	 * @throws Exception 
	 */
	public static String findSuidByFileName(String filename, String bookAuId) throws Exception {
		
		if ( filename.startsWith("localfile")) {
			filename = filename.replace("localfile:", "");
		}
		
		String query1 = "select pmd_object_id from a_su where a_au_id='" + bookAuId + "' and pmd_orig_file_name like '%" + filename + "' and pmd_status='Active'";
		String su_id = null;
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				su_id = rs.getString("pmd_object_id");
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return su_id;
	}
	
	

	public static String findSuidByFuid(String fu_id, String bookAuId) throws Exception {
		
		String query1 = "select pmd_object_id from a_su where pmd_fu_id='" + fu_id + "' and pmd_status='Active'";
		String su_id = null;
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				su_id = rs.getString("pmd_object_id");
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return su_id;
	}
	

	public static String findSuidByDOI(String doi, String bookAuId) throws Exception {
		
		String query1 = "select pmd_object_id from a_su where a_au_id='" + bookAuId + "' and pmd_status='Active' and pmd_mime_type='application/pdf' " 
						+ " and pmd_orig_file_name like '%" + doi + ".pdf%'";
		String su_id = null;
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				su_id = rs.getString("pmd_object_id");
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return su_id;
	}





	public static int findPdfFileCountFromDB(String bookAuId) throws Exception {
		int pdfCount = 0;
		
		String query1 = "select count(*) from a_su where a_au_id='" + bookAuId + "' and pmd_status='Active' and pmd_mime_type='application/pdf'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				pdfCount = rs.getInt(1);
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return pdfCount;
	}


	public static String findSuidByAuid(String bookAuId) throws Exception {
		//String query1 = "select pmd_object_id from a_su where a_au_id='" + bookAuId + "' and pmd_status='Active' and pmd_mime_type='application/pdf'" ;
		String query1 = "select a_su.pmd_object_id from a_cu, a_fu, a_su where a_cu.pmd_au_id=a_su.a_au_id and a_fu.pmd_cu_id=a_cu.pmd_object_id and "
						+ " a_su.pmd_fu_id=a_fu.pmd_object_id and a_su.a_au_id='" + bookAuId + "' and "
						+ " a_cu.pmd_status='Active' and a_su.pmd_status='Active' and a_su.pmd_mime_type='application/pdf'" ;


		String su_id = null;

		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {

			ResultSet rs = stmt.executeQuery(query1);


			if ( rs.next()) {
				su_id = rs.getString("pmd_object_id");
			}

			rs.close();

		}
		catch(Exception e) {
			throw e;
		}

		return su_id;
	}
	
	
	/**
	 * For Sage PDFs, choose the bigger size pdf.
	 * @param bookAuId
	 * @return
	 * @throws Exception 
	 */
	public static String findSuidByAuidWithBiggerSize(String bookAuId) throws Exception {
		String query1 = "select a_su.pmd_object_id, a_su.pmd_file_size from a_cu, a_fu, a_su where a_cu.pmd_au_id=a_su.a_au_id and a_fu.pmd_cu_id=a_cu.pmd_object_id and "
				+ " a_su.pmd_fu_id=a_fu.pmd_object_id and a_su.a_au_id='" + bookAuId + "' and "
				+ " a_cu.pmd_status='Active' and a_su.pmd_status='Active' and a_su.pmd_mime_type='application/pdf'" ;


		String su_id = null;
		int bigger_pdf_file_size = 0;

		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {

			ResultSet rs = stmt.executeQuery(query1);


			while ( rs.next()) {
				String suid = rs.getString("pmd_object_id");
				int pdf_file_size = rs.getInt("pmd_file_size");
				if ( pdf_file_size> bigger_pdf_file_size) {
					bigger_pdf_file_size= pdf_file_size;
					su_id = suid;
				}
			}

			rs.close();

		}
		catch(Exception e) {
			throw e;
		}

		return su_id;
	}


	


	/** Not in use
	 * 
	 * @param pdfFileWithPath
	 * @return
	 * @throws IOException
	 */
	public static PDDocumentOutline splitPdfByBookmarks(String pdfFileWithPath) throws IOException {
		
		PDDocumentOutline outline = null;
		PDDocument document = null;
		try {
			document = PDDocument.load(new File(pdfFileWithPath));
			outline =  document.getDocumentCatalog().getDocumentOutline();
			document.close();
		} catch (IOException e3) {
			logger.error(programName + ":splitPdfByBookmarks Exception while trying to load pdf document - " + e3.getMessage());

		}
		finally {
			if( document != null )
			   {
			      document.close();
			   }
		}
		
				
		return outline;
	}


	/**
	 * Get deduped ebook AU list of a publisher. If multiple holdings link to one unibook, the one whose AU has a later a_ingest_timestamp will be picked.
	 * @param conn
	 * @param publisher
	 * @param publisherBookCSName 
	 * @return AU_ID of the book's holding
	 * @throws Exception 
	 */
	public static List<String> getDedupedBookListForAPublisher(Connection conn, String publisher, String publisherBookCSName) throws Exception {
		List<String> book_auid_list = new ArrayList<>();

		
		String query = "with mytable as "+
						" ( "+
						"	select ana_unified_book.unibook_id, ana_unified_book.title, ana_unified_book.creator, " +
						"		ana_unibook_holding.holding_id, source, a_ingest_timestamp " +
						"	from ana_unified_book,  ana_unibook_holding, a_au " +
						"	where ana_unified_book.unibook_id=ana_unibook_holding.unibook_id and ana_unibook_holding.holding_id=a_au.pmd_object_id " +
						"		and content_set_name='" + publisherBookCSName + "' and is_series='no' " +
						" ), " +
						"ordered as " +
						" ( "+
						"	select unibook_id, title, holding_id, row_number() over (partition by unibook_id order by  a_ingest_timestamp desc ) as rn " +
						"	from mytable "+
						") " +
						"select unibook_id, title, holding_id from ordered where rn=1";
		
		try( Statement stmt = conn.createStatement();) {

			ResultSet rs = stmt.executeQuery(query);


			while ( rs.next()) {
				String book_auid = rs.getString("holding_id");
				book_auid_list.add(book_auid);
			}

			rs.close();
			
			System.out.println("Total deduped ebooks of " + publisherBookCSName + ": " + book_auid_list.size());

		}
		catch(Exception e) {
			throw e;
		}

		return book_auid_list;
	}


	public static String[] CoreNLPTokenize(String articleFullText) {
		// TODO Auto-generated method stub
		return null;
	}




	/**
	 * Calculate words frequency from tokens generated by CoreNLP
	 * https://www.baeldung.com/java-word-frequency
	 * @param words
	 * @return
	 */
	public static List<TokenCount> generateUnigramFromToken(List<String> words) {
		

		Map<String, Integer> counts =
				words.stream().map(String::toLowerCase).collect( Collectors.toMap(w -> w, w -> 1, Integer::sum) );
		
		Map<String, Long> wordCounter =
				words.stream().map(String::toLowerCase).collect( Collectors.groupingBy(Function.identity(), Collectors.counting()) );

		Map<String, Integer> intCounters =
				words.stream().map(String::toLowerCase).collect( Collectors.groupingBy(Function.identity(), Collectors.summingInt(val -> 1)) );
		
		Map<String, Long> counterMap = new HashMap<>();
		
		/*words.stream().map(String::toLowerCase).parallel()
				.collect(Collectors.groupingBy(k -> k, ()-> counterMap, Collectors.counting()));*/
		
		List<TokenCount> unigram = counts.entrySet().stream()
				.map(e -> new TokenCount(e.getKey(), e.getValue()))
				.collect(Collectors.toList());
		
		return unigram;
		 
	}
	
	

	public static Map<String, Integer> generateUnigramMapFromWords(List<String> words) {
		Map<String, Integer> counts =
				words.stream().map(String::toLowerCase).collect( Collectors.toMap(w -> w, w -> 1, Integer::sum) );
		return counts;
	}





	/**
	 * Return 3 letter lanaugage code.
	 * @param iso2LanCode
	 * @return Null if not found.
	 */
	public static String convertLan2ToLan3(String iso2LanCode) {

		String lan3 = null;
		
		if ( iso2LanCode != null && iso2LanCode.length() == 2 ) {
			lan3 = Lan2ToLan3Map.get(iso2LanCode.toLowerCase());
		}
		else if ( iso2LanCode.length() == 3 ) {
			lan3 = iso2LanCode;
		}

		return lan3;
	}


	public static List<String> queryDBForAllAUsOfContentSet(String cs) throws Exception {
		List<String> aulist = new ArrayList<>();
		
		String query = "select au_id from dw_article where issn_no='" + cs + "' and duplication_flag='N' "
				+ " order by all_to_number(vol_no), all_to_number(issue_no), sort_key, start_page_no" ;
		

		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {

			ResultSet rs = stmt.executeQuery(query);


			while ( rs.next()) {
				String au_id = rs.getString("au_id");
				aulist.add(au_id);
			}

			rs.close();

		}
		catch(Exception e) {
			throw e;
		}

		return aulist;
	}


	public static String gzipFile(String filePath) {
		
		String zipFileName = null;
		
		try {
            zipFileName = filePath + ".gz";
 
            FileOutputStream fos = new FileOutputStream(zipFileName);
            GZIPOutputStream zos = new GZIPOutputStream(fos);
 
            //zos.putNextEntry(new ZipEntry(file.getName()));
 
            byte[] bytes = Files.readAllBytes(Paths.get(filePath));
            zos.write(bytes, 0, bytes.length);
            //zos.closeEntry();
            zos.close();
 
        } catch (FileNotFoundException ex) {
            logger.error( programName + ":gzipFile: File doesn't exist " + filePath );
        } catch (IOException ex) {
        	logger.error( programName + ":gzipFile: I/O error: " + ex);
        }
		
		return zipFileName;
	}


	public static String normalizeUTF8String(String text) {
		
		//text = text.replace("\"", "\\\\\\\\\"");
		
		CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
		utf8Decoder.onMalformedInput(CodingErrorAction.REPLACE);
		utf8Decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);

		ByteBuffer bb = null;

		bb = ByteBuffer.wrap(text.getBytes());


		try {
			text = utf8Decoder.decode(bb).toString();
		} catch (CharacterCodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



		return text;
	}


	public static void printSQLException(SQLException ex) {

		for (Throwable e : ex) {
			if (e instanceof SQLException) {
				if (ignoreSQLException(  ((SQLException)e).getSQLState()) == false) {

					e.printStackTrace(System.err);
					logger.error("SQLState: " + ((SQLException)e).getSQLState());
					logger.error("Error Code: " + ((SQLException)e).getErrorCode());
					logger.error("Message: " + e.getMessage());

					Throwable t = ex.getCause();
					while(t != null) {
						System.out.println("Cause: " + t);
						t = t.getCause();
					}
				}
			}
		}
	}


	public static boolean ignoreSQLException(String sqlState) {

		if (sqlState == null) {
			System.out.println("The SQL state is not defined!");
			return false;
		}

		// X0Y32: Jar file already exists in schema
		if (sqlState.equalsIgnoreCase("X0Y32"))
			return true;

		// 42Y55: Table already exists in schema
		if (sqlState.equalsIgnoreCase("42Y55"))
			return true;

		return false;
	}

	/**
	 * This method add another single quote to escape single quote character in str. 
	 * @param thistitle
	 * @return
	 */
	public static String addSQLQuote(String str) {
		return str.replaceAll("'", "''");
	}


	public static String longestSubstring(String str1, String str2) {
		 
		StringBuilder sb = new StringBuilder();
		if (str1 == null || str1.isEmpty() || str2 == null || str2.isEmpty())
		  return "";
		 
		// ignore case
		str1 = str1.toLowerCase();
		str2 = str2.toLowerCase();
		 
		// java initializes them already with 0
		int[][] num = new int[str1.length()][str2.length()];
		int maxlen = 0;
		int lastSubsBegin = 0;
		 
		for (int i = 0; i < str1.length(); i++) {
			for (int j = 0; j < str2.length(); j++) {
				if (str1.charAt(i) == str2.charAt(j)) {
					if ((i == 0) || (j == 0))
						num[i][j] = 1;
					else
						num[i][j] = 1 + num[i - 1][j - 1];

					if (num[i][j] > maxlen) {
						maxlen = num[i][j];
						// generate substring from str1 => i
						int thisSubsBegin = i - num[i][j] + 1;
						if (lastSubsBegin == thisSubsBegin) {
							//if the current LCS is the same as the last time this block ran
							sb.append(str1.charAt(i));
						} else {
							//this block resets the string builder if a different LCS is found
							lastSubsBegin = thisSubsBegin;
							sb = new StringBuilder();
							sb.append(str1.substring(lastSubsBegin, i + 1));
						}
					}
				}
			}
		}
		 
		return sb.toString();
	}


	
	public static boolean almostSameTitle(String title1, String title2) {
		boolean result = false;
		
		if ( title1 == null || title2 == null) {
			return result;
		}
		
		//normalize titles: remove html tag, entities, remove ending period, fix wrong decoded unicode characters
		title1 = normalizeString(title1).toLowerCase().trim();
		title2 = normalizeString(title2).toLowerCase().trim();
		
		//de-accent
		title1 = convertNonAscii(title1);
		title2 = convertNonAscii(title2);	
		
		//remove puntuations
		title1 = removePunctuationMarks(title1);
		title2 = removePunctuationMarks(title2);
		
		String beforeEscapeTitle1 = title1;
		String beforeEscapeTitle2 = title2;
		
		if ( title1.equalsIgnoreCase(title2)) {  
			result = true;
			return result;
		}
		else if ( title1.startsWith(title2 + ":") || title2.startsWith(title1 + ":")) {		//one has subtitle
			result = true;
			return result;
		}
		else if ( title1.matches("^" + title2 + "\\s*:.*") || title2.matches("^" + title1 + "\\s*:.*")) {	//one has subtitle, delimited by space*:
			result = true;
			return result;
		}
		
		title1 = escapeRegexMetaChar( title1 );
		title2 = escapeRegexMetaChar( title2 );
		
		

		//different edition are considered same book if identifiers overlap
		Pattern p3 = Pattern.compile("^(.*?)(\\b\\w+\\b (ed|edition|auflage))?$");
		Matcher m21 = p3.matcher(title1.toLowerCase());
		Matcher m22 = p3.matcher(title2.toLowerCase());
		if ( m21.find() && m22.find()) {
			
			//System.out.println(m21.group(0) + ", " + m21.group(1) + ", " + m21.group(2) + ", " + m21.group(3)  );
			String title1_part1 = m21.group(1).trim();
			String title2_part1 = m22.group(1).trim();
			
			if ( title1_part1 != null && title2_part1 != null && title1_part1.equalsIgnoreCase(title2_part1)) {
				result = true;
				return result;
			}
			else if ( beforeEscapeTitle1.matches(beforeEscapeTitle2 + ",\\s+\\d") 
					|| beforeEscapeTitle2.matches(beforeEscapeTitle1 + ",\\s+\\d") ) { //is this safe without provider_id?
				//looks like edition, in Springer German ebooks. Ie.  'Soziologie, 2' vs 'Soziologie', 
				result = true;  
				return result;
			}
			else if ( beforeEscapeTitle2.replace(beforeEscapeTitle1, "").matches(",\\s+\\d")
					|| beforeEscapeTitle1.replace(beforeEscapeTitle2, "").matches(",\\s+\\d") ) {
				result = true;
				return result;
			}
		}
		
		
		
		int length1 = title1.length();
		int length2 = title2.length();
		int longerlength = (length1 > length2? length1: length2);
		if ( 10 * longestSubstring(title1, title2).length() / longerlength < 7 ) {    //matching substring is less than 70% of longer title length
			result = false;
			return result;
		}
		
		
		
		Pattern p1 = Pattern.compile(".*(vol|volume)(^\\d)*(\\d)+.*");
		Matcher m1 = p1.matcher(title1);
		Matcher m2 = p1.matcher(title2);
		if ( m1.find() && m2.find()) {
			String v1 = m1.group(3);
			String v2 = m2.group(3);
			if ( v1.equalsIgnoreCase(v2)) {
				result = true;
				return result;
			}
		}
		
		Pattern p2 = Pattern.compile(".*(vol|volume)\\.?(\\s)+(i|ii|iii|iv|v|vi|vii|viii|ix|x).*");
		m1 = p2.matcher(title1);
		m2 = p2.matcher(title2);
		if ( m1.find() && m2.find()) {
			String v1 = m1.group(3);
			String v2 = m2.group(3);
			if ( v1.equalsIgnoreCase(v2)) {
				result = true;
				return result;
			}
		}
		/*
		//last chance, get rid of those punctuation marks
		title1 = beforeEscapeTitle1;
		title2 = beforeEscapeTitle2;
		title1 = Utility.removePunctuationMarks(title1);
		title2 = Utility.removePunctuationMarks(title2);
		
		
		if ( title1.equalsIgnoreCase(title2)) {
			result = true;
			return result;
		}*/
		
		return result;
	}


	private static String removePunctuationMarks(String title1) {
		// TODO Auto-generated method stub
		return null;
	}


	private static String convertNonAscii(String title1) {
		// TODO Auto-generated method stub
		return null;
	}


	/**
	 * Remove html tags, replace html entities, ending period, double qoutation, fix wrongly decoded unicode characters
	 * @param title
	 * @return
	 */
	public static String normalizeString(String str) {
		
		if (str == null ) {
			return null;
		}
		
		str = str.replaceAll("\\<[^>]*>","").trim();		//remove html tags in title
		str = str.replaceAll("\\.$", "");					//ending period
		str = str.replace("&#39;", "'");					//Apostrophe
		str = str.replace("&#x0023;", "#");					//sharp sign
		str = str.replace("&#x00AE;", "\u00AE");			//registered trade mark
		str = str.replace("&amp;", "&");				
		str = str.replace("&#x0021;", "!");	
		str = str.replaceAll("^\\{", "").replaceAll("\\}$", "");		//remove { } (Elsevier supplying titles)
		str = str.replaceAll("\"", "");						//remove double qoutation
		
		str = fixWrongEncoding( str );
		
		//str = Utility.convertNonAscii(str);
		
		return str;
	}


	private static String fixWrongEncoding(String str) {
		String newStr = str;
		
		for(String key: charMap.keySet() ) {
			String value = charMap.get(key);
			newStr = newStr.replace(key, value);
		}

		return newStr;
	}



	private static String escapeRegexMetaChar(String text) {
		
		Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[{}()\\[\\].+*?^$\\\\|]");
		
		text = SPECIAL_REGEX_CHARS.matcher(text).replaceAll("\\\\$0");
		
		
		return text;
	}

	/**
	 * This method finds the unibook_id from ana_unibook_holding table
	 * @param au_id
	 * @return
	 * @throws Exception 
	 */
	public static int findUnibookIdFromAuId(String au_id) throws Exception {
		
		int unibook_id = 0;
		
		String query = "select unibook_id from ana_unibook_holding where holding_id='" + au_id + "' " ;
		

		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {

			ResultSet rs = stmt.executeQuery(query);


			if ( rs.next()) {
				unibook_id = rs.getInt("unibook_id");
			}

			rs.close();

		}
		catch(Exception e) {
			throw e;
		}
		
		return unibook_id;
	}


	public static boolean getOAStatusOfBookContentSet(String content_set_name)  throws Exception {
		
		boolean result = false;
		
		String query1 = "select has_oa from cmi_agreement_content_sets where content_type='E-Book Content' and content_set_name='" + content_set_name.trim() + "'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				String has_oa = rs.getString("has_oa");
				if ( has_oa.equalsIgnoreCase("yes")) {
					result = true;
				}
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return result;
	}
	
	public static boolean isHasOABookPublisher(String publisher) throws Exception {
		boolean result = false;
		
		String query1 = "select has_oa from cmi_agreement_content_sets where content_type='E-Book Content' and provider_id='" + publisher.toUpperCase().trim() + "'";
		
		try( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			
			ResultSet rs = stmt.executeQuery(query1);
			

			if ( rs.next()) {
				String has_oa = rs.getString("has_oa");
				if ( has_oa.equalsIgnoreCase("yes")) {
					result = true;
				}
			}
			
			rs.close();
			
		}
		catch(Exception e) {
			throw e;
		}
		
		return result;
	}





	public static List<String> getFullTextFromPDF(String pdfFileWithPath) throws Exception {
		
		List<String> pageTextList = new ArrayList<>();
		
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
						//remove Windows carige return characters
						text = text.replaceAll("\\r\\n", "\\n");
						
						//System.out.println(text);

						pageTextList.add(text);
					} catch (Exception e3) {
						logger.error( programName + ":getFullTextFromPDF : Error get text from split document : " + pdfFileWithPath + " " + e3);
						throw e3;
					}


				}

			} catch (Exception e3) {
				logger.error( programName + ":getFullTextFromPDF : Error split document with Splitter : " + pdfFileWithPath + " " + e3);
				throw e3;
			}
			finally {
				document.close();
			}

		} catch (Exception e3) {
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


	public static JSONObject getJsonObjectFromUrl(String json_url) throws Exception {
		
		
		URL url = null;
		try {
			url = new URL(json_url);
		} catch (MalformedURLException e1) {
			logger.error( programName + ":getJsonObjectFromUrl " + json_url + " " + e1.getMessage());
			throw new Exception("Unable to create URL with " + json_url);
		}
		HttpsURLConnection connection;
		try {
			connection = (HttpsURLConnection) url.openConnection();
		} catch (IOException e1) {
			logger.error( programName + ":getJsonObjectFromUrl " + json_url + " " + e1.getMessage());
			throw e1;
		}

		// parsing file 
		Object obj = null;
		try {
			obj = (JSONObject) new JSONParser().parse(new InputStreamReader(connection.getInputStream()));
		} catch (FileNotFoundException e) {
			logger.error( programName + ":getJsonObjectFromUrl " + json_url + " " + e.getMessage());
			throw e;
		} catch (IOException e) {
			logger.error( programName + ":getJsonObjectFromUrl " + json_url + " " + e.getMessage());
			throw e;
		} catch (org.json.simple.parser.ParseException e) {
			logger.error( programName + ":getJsonObjectFromUrl " + json_url + " " + e);
			throw e;
		} 

		// typecasting obj to JSONObject 
		JSONObject jo = (JSONObject) obj; 
		
		return jo;
	}


	/**
	 * Concatenate listOfJsonFiles into jsonlinefileWithPath
	 * @param listOfJsonFiles
	 * @param jsonlinefileWithPath
	 * @return
	 * @throws Exception 
	 */
	public static List<String> concatenateJsonFilesToOneFile(List<String> listOfJsonFiles, String jsonlinefileWithPath) throws Exception {
		
		List<String> missingFileList = new ArrayList<>();
		
	    // Charset for read and write
	    Charset charset = StandardCharsets.UTF_8;
	    
	    // Join files (lines)
	    for (String  jsonfile : listOfJsonFiles) {
	    	
	    	Path path = Paths.get( jsonfile );
	    	if (! Files.exists( path )) {
	    		logger.error( programName + ":concatenateJsonFilesToOneFile : File not exist " + jsonfile);
	    		missingFileList.add(jsonfile );
	    		continue;
	    	}
	    	
	        List<String> lines = null;
			try {
				lines = Files.readAllLines(path );
			} catch (IOException e1) {
				logger.error( programName + ":concatenateJsonFilesToOneFile : Error read lines from " + jsonfile + " " + e1.getClass().getSimpleName());
				throw new Exception("Error read json file");
			}
			
	        try {
				Files.write(  Paths.get(jsonlinefileWithPath), lines, charset, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.error( programName + ":concatenateJsonFilesToOneFile : Error concatenate to " + jsonlinefileWithPath + " " + e.getMessage());
				throw new Exception("Error appending to file");
			}

	    }
	    
	    return missingFileList;
	    
	}


	public static List<String> gzipFiles(List<String> jsonlinefiles) {
		List<String> gzipfiles = jsonlinefiles
				.stream()
				.map(s-> { return TDMUtil.gzipFile( s ); })
				.collect(Collectors.toList());
		
		return gzipfiles;
	}


	public static void uploadFiles(List<String> gzipJsonLineFile) {
		gzipJsonLineFile.stream().forEach(s-> {
			UploadGzipJsonLineFile( s );
		});
		
		
	}


	public static void UploadGzipJsonLineFile(String gzipJsonLineFile) {
		
		String BUCKET = "ithaka-labs";
		String V2_prefix = "tdm/v2/loading/portico/";
		String key = V2_prefix + Paths.get(gzipJsonLineFile).getFileName();
		
		S3AsyncClient client =  S3AsyncClient.builder()
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                					.connectionMaxIdleTime(Duration.ofSeconds(5)))
                .build();
		
		logger.info( programName + ":UploadGzipJsonLineFile " + gzipJsonLineFile + " to S3 s3://" + BUCKET + "/" + key );

		CompletableFuture<PutObjectResponse> future = client.putObject(
				PutObjectRequest.builder()
				.bucket(BUCKET)
				.key(key)
				.contentEncoding("gzip")
				.build(),
				AsyncRequestBody.fromFile(Paths.get( gzipJsonLineFile))
		);
		
		future.whenComplete((resp, err) -> {

			if (resp != null) {
				//logger.info( programName + ":UploadObject " + resp  );
			} else {
				logger.error( programName + ":UploadGzipJsonLineFile Error uploading " + gzipJsonLineFile + " " + err.getMessage()  );
				throw new RuntimeException(err);
			}
		});

		future.join();
		
	}


	public static void logNewspaperIssueUploadTimeToDB(List<String> jsonFileList) {

		String update_query = "update tdm_newspaper set upload_ts=current_timestamp where lccn=? and issue=? and edition=?";
		Pattern p = Pattern.compile( "(.*)_(\\d{4}-\\d{2}-\\d{2})_ed-(\\d).json");
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				PreparedStatement update_stmt = conn.prepareStatement(update_query); ) {
			
			for(String jsonfile: jsonFileList) {
				//[path] + getLccn() + "_" + getDatePublished() + "_ed-" + getEdition() + ".json";
				String lccn=null;
				String issue=null;
				String edition=null;
				
				Matcher m = p.matcher(jsonfile.substring(jsonfile.lastIndexOf(File.separator) + 1 ));
				if ( m.find()) {
					lccn = m.group(1);
					issue = m.group(2);
					edition = m.group(3);
					
				}
				else {
					logger.error( programName + ":logNewspaperIssueUploadTimeToDB : Error matching issue parameters " + jsonfile);
					continue;
				}
				
				update_stmt.setString(1, lccn);
				update_stmt.setString(2, issue);
				update_stmt.setString(3, edition);
				
				try {
					update_stmt.executeUpdate();
					//logger.info( programName + ":logArticleUploadTimeToDB " + update_query + " " + au_id );
				}
				catch(SQLException e) {
					logger.error( programName + ":logNewspaperIssueUploadTimeToDB: Error updating TDM_NEWSPAPER:upload_ts for " + lccn + " " + issue + " " + e);
				}
			}
		}
		catch(  Exception  e) {
			logger.error( programName + ":logNewspaperIssueUploadTimeToDB:  " + e);
			e.printStackTrace();
		}
		
		
		
	}


	public static void transformXml(String inputXmlFile, String outputFile, String xslFile) throws Exception {
		try {
        	
            // Create transformer factory
            TransformerFactory tfactory = TransformerFactory.newInstance();
            
            // Use the factory to create a template containing the xsl file
            Templates template = tfactory.newTemplates(new StreamSource(
                new FileInputStream(xslFile)));

            // Use the template to create a transformer
            Transformer xformer = template.newTransformer();

            // Prepare the input and output files
        	DocumentBuilderFactory dfactory = DocumentBuilderFactory.newInstance();
        	dfactory.setValidating(false);
        	dfactory.setNamespaceAware(true);
        	dfactory.setFeature("http://xml.org/sax/features/namespaces", false);
        	dfactory.setFeature("http://xml.org/sax/features/validation", false);
        	dfactory.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
        	dfactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        	DocumentBuilder docbuilder = dfactory.newDocumentBuilder();
        	Document doc = docbuilder.parse(new FileInputStream(inputXmlFile));
        	
        	Source source = new DOMSource(doc.getDocumentElement());
            Result result = new StreamResult(new FileOutputStream(outputFile));
            

            // Apply the xsl file to the source file and write the result
            // to the output file
            xformer.transform(source, result);
            
            
        } catch (FileNotFoundException e) {
        	throw e;
        } catch (TransformerConfigurationException e) {
            throw e;
        } catch (TransformerException e) {
            // An error occurred while applying the XSL file
            // Get location of error in input file
            SourceLocator locator = e.getLocator();
            int col = locator.getColumnNumber();
            int line = locator.getLineNumber();
            String publicId = locator.getPublicId();
            String systemId = locator.getSystemId();
        } catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		} catch (SAXException e) {
			
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			throw e;
		}

		
	}


	public static String getPublisherFromAU(String auid) throws Exception {
		String publisherID = "";

		String query = "select provider_id from a_au, cmi_agreement_content_sets where pmd_object_id='" + auid + "'" 
				+ " and a_au.pmd_content_set_name=cmi_agreement_content_sets.content_set_name";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			if( rs.next() ) {

				publisherID = rs.getString("provider_id");

			}
			
			rs.close();
		}
		catch(Exception e) {
			logger.error( programName + ":getPublisherFromAU " + e.getMessage());
			throw e;
		}
		
		return publisherID;
	}


	public static String getContentSetNamefromAU(String auid) throws Exception {
		String CSName = null;

		String query = "select pmd_content_set_name from a_au where pmd_object_id='" + auid + "'";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			if( rs.next() ) {

				CSName = rs.getString("pmd_content_set_name");

			}
			
			rs.close();
		}
		catch(Exception e) {
			logger.error( programName + ":getContentSetNamefromAU " + e.getMessage());
			throw e;
		}
		
		return CSName;
	}

	/**
	 * Read AU list from inputFile
	 * @param inputFile relative path with input file name
	 * @return
	 * @throws IOException
	 */

	public static List<String> readAUListFromFile(String inputFile) throws IOException {
		List<String> lists = new ArrayList<>();
		
		//read AUs from file
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


	public static void deleteDir(File file) {
		File[] contents = file.listFiles();
	    if (contents != null) {
	        for (File f : contents) {
	            if (! Files.isSymbolicLink(f.toPath())) {
	                deleteDir(f);
	            }
	        }
	    }
	    file.delete();
	}


	
	public static String getPublisherIdfromContentSetName(String cs) throws Exception {
		String publisher_id = null;

		String query = "select provider_id from cmi_agreement_content_sets where content_set_name='" + cs + "'";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			if( rs.next() ) {

				publisher_id = rs.getString("provider_id");

			}
			
			rs.close();
		}
		catch(Exception e) {
			logger.error( programName + ":getPublisherIdfromContentSetName " + e.getMessage());
			throw e;
		}
		
		return publisher_id;
	}


	/**
	 * This method get publisher ids from tdm_publisher table.
	 * @return
	 * @throws Exception 
	 */
	public static List<String> getTDMJournalPublishers() throws Exception {
		
		List<String> publisher_ids = new ArrayList<>();

		String query = "select publisher_id from tdm_publisher where journal_count>=1 order by publisher_id";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			while( rs.next() ) {

				String publisher_id = rs.getString("publisher_id");
				publisher_ids.add(publisher_id);

			}
			
			rs.close();
		}
		catch(Exception e) {
			logger.error( programName + ":getTDMJournalPublishers " + e.getMessage());
			throw e;
		}
		
		return publisher_ids;
	}


	public static List<String> getTDMBookPublishers() throws Exception {
		List<String> publisher_ids = new ArrayList<>();

		String query = "select publisher_id from tdm_publisher where book_count>=1 order by publisher_id";
		
		try ( Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				Statement stmt = conn.createStatement();) {
			ResultSet rs = stmt.executeQuery(query);

			while( rs.next() ) {

				String publisher_id = rs.getString("publisher_id");
				publisher_ids.add(publisher_id);

			}
			
			rs.close();
		}
		catch(Exception e) {
			logger.error( programName + ":getTDMBookPublishers " + e.getMessage());
			throw e;
		}
		
		return publisher_ids;
	}


	/**
	 * Create cs list for publisher to config/publisherID_cs_list
	 * @param conn
	 * @param pubisherID, ie IAOE
	 * @return config/IAOE_cs_list
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public static String createPublisherCSList(Connection conn, String publisherID) throws IOException, SQLException {
		
		String fileName =  "config" + File.separator + publisherID.toLowerCase() + "_cs_list";
		
		logger.info( programName + ":createPublisherCSList creating content set list for " + publisherID + " to " + fileName + " ...");
		
		String query = "select issn_no from dw_journal where publisher_id='" + publisherID + "' and  jnl_completeness_status is not null order by issn_no";
		List<String> cs_list = new ArrayList<>();
		
		try ( Statement stmt = conn.createStatement() ) {

			ResultSet rs = stmt.executeQuery(query);
		
			while( rs.next()) {
				
				String cs_name = rs.getString("issn_no");

				cs_list.add(cs_name);
			}
			
			rs.close();
			
		}
		catch(SQLException sqle) {
			logger.error( programName + ":createPublisherCSList " + query + ". "  + sqle.getMessage());
			throw sqle;
		}
		
		//output to list_file_name
		Path filePath = Paths.get( fileName );
		 
		try {
			Files.write(filePath, cs_list);
		} catch (IOException e) {
			logger.error( programName + ":createPublisherCSList Error write cs list to file " + fileName + " "+ e.getMessage());
			throw e;
		}
		
		return fileName;
	}


	

	
	
	
	
	
}

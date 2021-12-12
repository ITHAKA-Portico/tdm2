package org.portico.tdm.tdm2.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.SourceLocator;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class XslTransform {

	public static void main(String[] args) {
		String inputXmlFile = "input" + File.separator + "docsouth" + File.separator + "fpn" + File.separator + "data" + File.separator + "xml" + File.separator + "fpn-ball-ball.xml";
		String outputFile = "output" + File.separator + "docsouth" + File.separator + "fpn" + File.separator + "ball.output";
		String xslFile = "input" + File.separator + "docsouth" + File.separator + "fpn" + File.separator + "data" + File.separator + "text-only.xsl";
		
		XslTransform.xsl(inputXmlFile, outputFile, xslFile);

	}
	
	 public static void xsl(String inFilename, String outFilename, String xslFilename)   {
	        try {
	        	
	            // Create transformer factory
	            TransformerFactory tfactory = TransformerFactory.newInstance();
	            
	            // Use the factory to create a template containing the xsl file
	            Templates template = tfactory.newTemplates(new StreamSource(
	                new FileInputStream(xslFilename)));

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
	        	Document doc = docbuilder.parse(new FileInputStream(inFilename));
	        	
	        	Source source = new DOMSource(doc.getDocumentElement());
	            Result result = new StreamResult(new FileOutputStream(outFilename));
	            

	            // Apply the xsl file to the source file and write the result
	            // to the output file
	            xformer.transform(source, result);
	            
	            
	        } catch (FileNotFoundException e) {
	        } catch (TransformerConfigurationException e) {
	            // An error occurred in the XSL file
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
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }

}

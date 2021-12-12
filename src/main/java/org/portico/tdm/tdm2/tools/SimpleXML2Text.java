package org.portico.tdm.tdm2.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



/**
 * Strip out xml tags.
 * @author dxie
 *
 */
public class SimpleXML2Text {
	static Logger logger = LogManager.getLogger(SimpleXML2Text.class.getName());
	final static String inputDir = "input/";
	final static String outputDir = "output/";
	final String programName = "SimpleXML2Text";

	public static void main(String[] args) {
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();

		SimpleXML2Text converter = new SimpleXML2Text();

		// parse the command line arguments
		try {
			CommandLine line = parser.parse( options, args );
			String auid;
			String xmlfile;

			if ( line.hasOption("auid")) {
				auid = line.getOptionValue("auid");

				converter.convertAU2Text( auid );
			}
			else if ( line.hasOption("xml")) {
				xmlfile = line.getOptionValue("xml");

				converter.convertXML2Text(xmlfile);
			}
			else {
				SimpleXML2Text.printUsage( options );
			}


		}
		catch(Exception e) {

		}

	}


	private void convertAU2Text(String auid) {
		// TODO Auto-generated method stub

	}

	/**
	 * This method simple strip all xml tags. Save output to output/ directory
	 * @param xmlfile
	 */
	private String convertXML2Text(String xmlfile) {
		// read in xmlfile
		Path path = FileSystems.getDefault().getPath(inputDir, xmlfile);
		String fileContent = null;
		try {
			fileContent = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error( programName + ":convertXML2Text " + e.getMessage());
			e.printStackTrace();
		}
		
		//convert
		String newContent = fileContent.replaceAll("<[^>]+>", "");
		System.out.println(newContent);
		
		return newContent;

	}

	static String convertStreamToString(java.io.InputStream is) {
		if (is == null) return "";
		try(java.util.Scanner s = new java.util.Scanner(is)) { 
			return s.useDelimiter("\\A").hasNext() ? s.next() : ""; 
		}
	}

	private static void printUsage(Options options) {

		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "SimpleXML2Text", options);  
		writer.flush();  

	}


	private static Options constructOptions() {

		final Options options = new Options();

		options.addOption(Option.builder("auid").required(false).hasArg().desc("").build() );
		options.addOption(Option.builder("xml").required(false).hasArg().desc("One single xml file to process").build() );

		return options;
	}


	public static String getFullText(String filename) {
		Path path = FileSystems.getDefault().getPath( inputDir, filename);
		String fileContent = null;
		try {
			fileContent = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
			
		}
		
		//convert
		String newContent = fileContent.replaceAll("<[^>]+>", "");
		//System.out.println(newContent);
		
		return newContent;
		
	}

	
}

package org.portico.tdm.tdm2.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;



/**
 * This class is used to random select certain number of files from input file
 * Case 1. Input file config/newspaers_titles.xlsx
 * Case 2, Input file config/OCR_batch_list.xlsx
 * 
 * Usage:
 *  java org.portico.tdm.tools.SampleChroniclingAmerica  -size 20 -file OCR_batch_list.xlsx
 * @author dxie
 *
 */
public class SampleChroniclingAmerica {
	
	static Logger logger = LogManager.getLogger(SampleChroniclingAmerica.class.getName());
	
	String sampleFilename;
	int sampleSize = 0;
	final String programName = "SampleChroniclingAmerica";
	
	private DataFormatter formatter = null;
    private FormulaEvaluator evaluator = null;
	

	public static void main(String[] args) {
		
		SampleChroniclingAmerica sampler = new SampleChroniclingAmerica();
		
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructPosixOptions();
		String file = null;
		String size = null;
		
		
		try {
			CommandLine line = parser.parse( options, args );

			if ( line.hasOption("file")) {		
				file = line.getOptionValue("file");
			}
			else {
			}
			
			if ( line.hasOption("size")) {
				size = line.getOptionValue("size");
			}
			else {
				printUsage(options);
			}
			

			try {
				sampler.sample(file, size);
			} catch (Exception e) {
				logger.error("SampleChroniclingAmerica file " + file + " " + e.getMessage());
				e.printStackTrace();
				System.exit(1);
			}


		} catch (ParseException e) {
			logger.error("SampleChroniclingAmerica: " + e.getMessage());
		}

			

	}

	private void sample(String file, String size) throws Exception {
		
		//convert size to int
		if ( size != null && ! size.isEmpty()) {
			sampleSize = new Integer(size).intValue();
		}
		
		//read in input file
		File inputFile = new File( "config" + File.separator + file );
		
		if ( ! inputFile.exists() || ! inputFile.canRead() ) {
			logger.error( programName + ": " + inputFile.getName() + " cannot be read or does not exist.");
			throw new Exception("Error read " + file );
		}
		
		//Read in input sheet
		Workbook wb;
		try {
			wb = readInputFileWorkbook(inputFile);
		} catch (Exception e2) {
			logger.error( programName + ": Cannot read input file " + e2.getMessage());
        	throw new Exception("Cannot read input file");
		}
		
        Sheet sheet = wb.getSheetAt(0);
        Row row = sheet.getRow(0);		//header row
        
        int lastRowNum = sheet.getLastRowNum();		
        int[] selectedRows = new int[sampleSize];
        
        logger.info( programName + ": random select " + sampleSize + " rows from " + lastRowNum + " rows from " + inputFile.getName());
        
        Random rand = new Random();
        for ( int i=0; i< sampleSize; i++) {
        	int randomInt = rand.nextInt(lastRowNum);
        	selectedRows[i] = randomInt;
        	
        	System.out.print( (i+1) + ", select row #" + randomInt + ": ");

        	row = sheet.getRow(randomInt);
        	if ( row == null ) {
        		continue;
        	}
        	
        	//print selected row
        	for (int j =0; j< row.getLastCellNum(); j ++) {
        		String value = getCellValue(row.getCell(j));
        		if ( value != null && ! value.isEmpty()) {
        			System.out.print("\t" + value );
        		}
        		else {
        			System.out.print("\t");
        		}
        	}
        	
        	System.out.println();

        }
        	
        	
	}

	private String getCellValue(Cell cell) {			

		String celldata = "";

		if ( cell != null) {
			
			CellType cellType = cell.getCellTypeEnum();
			switch(cellType) {
			case FORMULA:
				// this returns formula, not value celldata = cell.getCellFormula();
				switch(cell.getCachedFormulaResultTypeEnum()) {
				case NUMERIC:
					celldata =  "" + cell.getNumericCellValue();
					break;
				case STRING:
					celldata =  cell.getRichStringCellValue().toString();
					break;
				default:
					break;
				}
				break;
			case NUMERIC:
				if (DateUtil.isCellDateFormatted(cell)) {

					//solution 1 - works
					/*celldata = this.formatter.formatCellValue(cell, this.evaluator);   // 9/22/14
                Date celldate = null;
				try {
					celldate = new SimpleDateFormat("MM/dd/yy").parse(celldata);
				} catch (java.text.ParseException e) {
					logger.error("Cannot convert Date type in cell " + celldata);
				}
                DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
                celldata = df.format(celldate);*/

					//solution 2 - works
					String celldatestr = cell.getDateCellValue().toString();   //Mon Sep 22 00:00:00 EDT 2014

					try {
						Date celldate = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy").parse(celldatestr);
						celldata = new SimpleDateFormat("MM/dd/yyyy").format(celldate);
					} catch (java.text.ParseException e) {
						System.out.println("Cannot convert Date type in cell " + celldatestr);
					}
				} else {
					celldata = this.formatter.formatCellValue(cell, this.evaluator);
				}

				break;
			case STRING:
				celldata = cell.getStringCellValue();
				break;
			case BOOLEAN:
				celldata = Boolean.toString(cell.getBooleanCellValue());
				break;
			case BLANK:
				celldata = null;
				break;
			default:
				System.out.println("Cell type is: " + cell.getCellTypeEnum());
			}

		}
		else {
			celldata = "";
		}

		return celldata;
	}

	private Workbook readInputFileWorkbook(File inputFile) throws EncryptedDocumentException, IOException, InvalidFormatException {
		//read in file content
        FileInputStream fis = null;

        logger.info( programName + ": Opening workbook [" + inputFile.getName() + "]");

        fis = new FileInputStream(inputFile);
        Workbook masterwb = WorkbookFactory.create(fis);
        this.evaluator = masterwb.getCreationHelper().createFormulaEvaluator();
        this.formatter = new DataFormatter(true);
        
        return masterwb;
	}
	
	


	private static Options constructPosixOptions() {
		final Options options = new Options();
		
		options.addOption( "file", true, "The name of the file that contains content set name list to check short codes");
		options.addOption( "size", true, "The count of the sample size" );

		
		return options;
	}
	
	
	private static void printUsage(Options options) {
		
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, "Chorus", options);  
		writer.flush();  
		
	}



}

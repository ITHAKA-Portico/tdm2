package org.portico.tdm.tdm2.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used for caching Portico active xml files for future easy access.
 * It moves active xml file for AU to cache directory (data/portico_cache) and deletes AU subdirectory.
 * Usage:
 *   java org.portico.tdm.tools.CachePorticoAU -scandir input/sampledata [-delete]
 *   java org.portico.tdm.tools.CachePorticoAU -delete  -aufile tdm_au_1 > /dev/null 2>&1 
 *   java org.portico.tdm.tools.CachePorticoAU -combdir portico_cache_1
 * 
 * @author dxie
 *
 */
public class CachePorticoAU {
	static Properties props = new Properties();
	static Logger logger = LogManager.getLogger(CachePorticoAU.class.getName());
	static String programName = "CachePorticoAU";
	
	String configDir = "config";
	String inputDir = "input" + File.separator + "sampledata";  	//default source input dir
	String cacheDir = "data" + File.separator + "portico_cache";	//default cache dir

	boolean deleteFlag = false;
	
	String server = "PROD";
	
	long starttime, endtime;
	String startTimeStr, endTimeStr;
	String argStr;
	
	List<String> processedAUList;
	List<String> notProcessedAUList;
	
	public CachePorticoAU() {
		
		//get startTime
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		startTimeStr = dateFormat.format(cal.getTime());
		starttime = System.currentTimeMillis();
		
		processedAUList = new ArrayList<>();
		notProcessedAUList = new ArrayList<>();
	}

	public static void main(String[] args) {
		// create the command line parser
		final CommandLineParser parser = new DefaultParser();

		// create the Options
		final Options options = constructOptions();

		try {
			
			CachePorticoAU processor = new CachePorticoAU(  );
			
			
			CommandLine line = parser.parse( options, args );
			
			logger.info( programName + " with args: " + String.join(" ", args));
			processor.setArgStr( String.join(" ", args)) ;
			
			if ( line.hasOption("delete")) {
				processor.setDeleteFlag(true);
			}
			else {
				processor.setDeleteFlag(false);
			}
			
			if ( line.hasOption("scandir")) {
				String input_dir  = line.getOptionValue("scandir");
				processor.processDirectory(input_dir);
			}
			else if ( line.hasOption("aufile")) {
				String aufile  = line.getOptionValue("aufile");
				processor.processAUFile(aufile);
			}
			else if ( line.hasOption("combdir")) {		//one time job
				String cache_dir = line.getOptionValue("combdir");
				processor.updateCacheDir(cache_dir);
			}
			else {
				processor.printUsage( options );
			}
			
			processor.printStats();
			
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * This method updates each AU's tdm_au.cache_dir information for AUs in the cache_dir.
	 * This is a retrospective work to be run on server.
	 * @param cache_dir	ie, portico_cache_1
	 */
	private void updateCacheDir(String cache_dir) {

		//get All AUs under cache_dir
		String cache_dir_path = "data/" + cache_dir;
		
		List<String> au_list = scanDirGetAUList(cache_dir_path);  //AUs are without prefix, around 1M
		logger.info(programName + ":updateCacheDir Total " + au_list.size() + " AUs are under " + cache_dir_path);
		
		//split the au_list into chunks of 5000
		int targetSize = 5000;
		List<List<String>> chunks = ListUtils.partition(au_list, targetSize);
		
		int count = 0;
		for(List<String> one_chunk_of_aus: chunks) {
			count += updateCacheDirForListOfAUs( one_chunk_of_aus, cache_dir);
		}
		
		logger.info( programName + ":updateCacheDir Total " + count + " AUs have been updated");

	}


	/**
	 * 
	 * @param au_list
	 * @param cache_dir
	 * @return 
	 */
	private int updateCacheDirForListOfAUs(List<String> au_list, String cache_dir) {
		
		String query = "update tdm_au set cache_dir='" + cache_dir + "' where au_id=?";

		int count =  0;
		try (Connection conn = TDMUtil.getDWConnection_pooled("PROD");
				PreparedStatement stmt=  conn.prepareStatement(query);  ) {


			for(String au: au_list) {
				String au_id = "ark:/27927/" + au;
				stmt.setString(1, au_id);

				try {
					stmt.execute();

					count++;
				}
				catch(Exception e) {
					logger.error( programName + ":updateCacheDir for " + au + " with " + cache_dir + " " + e.getMessage());
				}

			}
		}
		catch(Exception e) {
			logger.error( programName + ":updateCacheDir with " + cache_dir + " " + e.getMessage());
		}

		logger.info( programName + ":updateCacheDir Total " + count + " AUs have been updated");
		
		return count;
		
	}

	/**
	 * Read au list from file in config/ directory. Then cache AU xml file to cache directory.
	 * @param filename ie tdm_au_1  (under config/)
	 */
	private void processAUFile(String filename) {
		
		logger.info( programName + ":processAUFile " + filename);
		List<String> AUlist = null;

		String inputFile = getConfigDir() + File.separator + filename;
		
		try {
			AUlist = TDMUtil.readAUListFromFile(inputFile);
		} catch (IOException e) {
			logger.error( programName + ":processAUFile " + inputFile + " " + e.getMessage());
			e.printStackTrace();
		}
		
		int i =1;
		for(String auid: AUlist) {
			try {
				cacheOneAU( auid , AUlist.size(), i );
				processedAUList.add(auid);
				i++;
			}
			catch(Exception e) {
				logger.error( programName + ":processAUFile: " + auid + " " + e.getMessage());
				notProcessedAUList.add(auid);
			}
		}
		
		
	}


	/**
	 * Scan directory. Copy xml file to cache directory, delete current directory.
	 * @param input_dir
	 */
	private void processDirectory(String input_dir) {
		
		setInputDir(input_dir);
		
		//get AU list
		List<String> au_list = scanDirGetAUList( input_dir );		//this au list doesn't have ark id
		
		int i = 1;
		for(String au: au_list) {
			try {
				cacheOneAU( au, au_list.size(), i );
				processedAUList.add(au);
				i++;
			} catch (Exception e) {
				logger.error( programName + ":processDirectory: " + e.getMessage());
				notProcessedAUList.add(au);
			}
		}
		
	}




	/**
	 * Copy active xml file of an AU from input dir to cache dir. Delete AU subdirectory under input dir if deleteFlag is set.
	 * @param au Could be with or without ark id. phx4r64rfrs
	 * @param index 
	 * @param total_count 
	 * @throws Exception 
	 */
	private void cacheOneAU(String au, int total_count, int index) throws Exception {
		
		//query DB to find out active XML file name
		String bitsXmlFileName = null;
		
		//we want au without ark id
		au = au.replace("ark:/27927/", "");
		
		//auid with ark id
		String auid = au;
		
		if ( ! auid.startsWith("ark")) {
			auid = "ark:/27927/" + auid;
		}
		
		try {
			bitsXmlFileName = TDMUtil.getBitsXmlFileNameForAU(auid);
		} catch (Exception e) {
			logger.error( programName + ":cacheOneAU: Error getting BITS xml file for " + auid);
			e.printStackTrace();
			throw e;
		}
		
		
		//constructs full xml file name
		String fullBitsXmlFileName = getInputDir() +  File.separator + au + File.separator + "data" + File.separator + bitsXmlFileName;
		String cacheXmlFileDir = getCacheDir() + File.separator  + au + File.separator + "data";
		
		try {
			Files.createDirectories(Paths.get(cacheXmlFileDir));
		} catch (IOException e1) {
			logger.error( programName + ": cacheOneAU: Error creating new directory " + cacheXmlFileDir + " " + e1.getMessage());
			throw e1;
		}
		
		String cacheXmlFileName = cacheXmlFileDir + File.separator + bitsXmlFileName;
		
        // copy xml file to cache directory 
		try {
			Files.copy(Paths.get(fullBitsXmlFileName), Paths.get(cacheXmlFileName), StandardCopyOption.REPLACE_EXISTING);
			
		} catch (IOException e) {
			logger.error( programName + ": cacheOneAU: Error moving file from " + fullBitsXmlFileName + " to " + cacheXmlFileName + " " + e.getMessage());
			throw e;
		}
        
		//delete original AU subdirectory
		if ( isDeleteFlag() ) {
			TDMUtil.deleteDir(new File(getInputDir() +  File.separator + au));
		
			logger.info( index + " of " + total_count + ", moved " + fullBitsXmlFileName + " to " + cacheXmlFileName + " and deleted " + (getInputDir() +  File.separator + au) + " directory." );
		}
		else {
			logger.info(index + " of " + total_count + ", moved " + fullBitsXmlFileName + " to " + cacheXmlFileName + ".");
		}
		
	}





	/**
	 * Get AUs under input_dir
	 * @param input_dir
	 * @return 	AUs without ark prefix
	 */
	private List<String> scanDirGetAUList(String input_dir) {
		
		List<String> AUs = new ArrayList<>();

		File[] directories = new File(input_dir).listFiles(File::isDirectory);
		
		for(File subdir: directories) {
			String dirname = subdir.toString();			//input\\sampledata\\phx4r64rfrs
			String AU = dirname.substring(dirname.lastIndexOf(File.separator)+1);		//phx4r64rfrs
			AUs.add(AU);
		}
		
		return AUs;
		
	}


	private void printStats() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance(  );
		endTimeStr = dateFormat.format(cal.getTime());
		endtime = System.currentTimeMillis();

        long totaltime = endtime -starttime;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd HH:mm:ss"); 
        String timeUsedStr = dateFormat2.format(new java.util.Date(totaltime));
        
        //print stats
        logger.info("===============================================================");
        logger.info( programName + " with args: " + getArgStr());
        logger.info("Processing summary: " );
        logger.info("Successfully cached xml file for AUs (" + processedAUList.size() + "): " );
       	logger.info( String.join(", ", processedAUList));
        
       	logger.info("Failed to cache xml file for AUs (" + notProcessedAUList.size() + "): " );
       	logger.info( String.join(", ", notProcessedAUList));
        
        logger.info("Total process time " + startTimeStr + "[start] to " + endTimeStr + "[end], in total = " + timeUsedStr );
        logger.info("================================================================");
        
		
	}


	private void printUsage(Options options) {
		final PrintWriter writer = new PrintWriter(System.out);
		final HelpFormatter usageFormatter = new HelpFormatter();  
		usageFormatter.printUsage(writer, 80, programName, options);  
		writer.flush();  
	}





	private static Options constructOptions() {
		final Options options = new Options();

		options.addOption(Option.builder("scandir").required(false).hasArg().desc("Scan directory of AUs. Copy xml file to cache directory.").build() );
		options.addOption(Option.builder("delete").required(false).desc("Delete AU subdirectory after copied xml file to cache directory").build() );
		options.addOption(Option.builder("aufile").required(false).hasArg().desc("File has AUs to cache. Copy xml file to cache directory.").build() );
		options.addOption(Option.builder("combdir").required(false).hasArg().desc("Update tdm_au.cache_dir for AUs in the cache directory.").build() );
		
		return options;
	}
		
	public String getInputDir() {
		return inputDir;
	}

	public void setInputDir(String inputDir) {
		this.inputDir = inputDir;
	}



	public String getArgStr() {
		return argStr;
	}

	public void setArgStr(String argStr) {
		this.argStr = argStr;
	}


	public boolean isDeleteFlag() {
		return deleteFlag;
	}

	public void setDeleteFlag(boolean deleteFlag) {
		this.deleteFlag = deleteFlag;
	}

	public String getCacheDir() {
		return cacheDir;
	}

	public void setCacheDir(String cacheDir) {
		this.cacheDir = cacheDir;
	}

	public String getConfigDir() {
		return configDir;
	}

	public void setConfigDir(String configDir) {
		this.configDir = configDir;
	}




}

package org.portico.tdm.tdm2.tools;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;


import org.portico.conprep.util.archiveexport.ExportAuBagsExploded;
import org.portico.tdm.util.ExportAu;

public class Test {
	
	final String ARMURL = "http://pr2ptgpprd04.ithaka.org:8095/repository/au/v1/locate";
	final String RETRY_ATTEMPTS = "3";
	final String READ_TIMEOUT = "1800000";
	final String CONNECTION_TIMEOUT = "60000";
	final String DATABASE_URL = "jdbc:oracle:thin:@pr2ptcingestora02.ithaka.org:1525:PPARCH1";
	final String DATABASE_USER = "archivemd2ro";
	final String DATABASE_PWD = "archivemd2ro";
	final String CONTENT_TYPE = "E-Journal Content";
	final String DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";


	public static void main(String[] args) {
		
		
        Test test = new Test();
        
        //test.test1();
        
 /*       try {
			test.test3();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        try {
			test.test2_processbuilder();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  */
        
        test.test4_exportAu();
	}

	
	
	private void test4_exportAu() {
		
		ExportAu exportor = new ExportAu();
		
		exportor.setContentType(CONTENT_TYPE);
		exportor.setInputType("ContentSetList");
		exportor.setInputValue("config/pub_cs_list");
		String proj_dir = System.getProperty("user.dir");
		exportor.setDestFolder("file:" + proj_dir + "/input/newcontent_202107");
		exportor.setIngestDateFrom("NA");
		exportor.setIngestDateTo("NA");
		//exportor.setAuContentModifiedDateFrom("03/01/2021");
		//exportor.setAuContentModifiedDateTo("08/01/2021");
		
		exportor.setDestFolder("file:" + proj_dir + "/input/newcontent_202108");
		exportor.setAuContentModifiedDateFrom("08/01/2021");
		exportor.setAuContentModifiedDateTo("09/01/2021");
		
		try {
			exportor.exportJournalAUFromCSList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	private void test3() throws IOException {
		Process process1 = new ProcessBuilder("/Library/Java/JavaVirtualMachines/jdk-11.0.2.jdk/Contents/Home/bin/java", "-version").start();
		
		inheritIO(process1.getInputStream(), System.out);
	    inheritIO(process1.getErrorStream(), System.err);
	    

	}

	
	/**
	 * This method is not working. And CLASSPATH has not been set up correctly.
	 * @throws IOException
	 */
	private void test2_processbuilder() throws IOException {
		
		ExportAuBagsExploded test = new ExportAuBagsExploded();
		
		String[] straJavaArgs =
			{
					"/Library/Java/JavaVirtualMachines/jdk-11.0.2.jdk/Contents/Home/bin/java",
					"-Xms256M",
					//"-Xmx512M",
					"org.portico.conprep.util.archiveexport.ExportAuBagsExploded",
					"-cp"
			};
		
		String proj_dir = System.getProperty("user.dir");
		
		String export_args[] = new String[14];
		List<String> csListFiles = new ArrayList<String>(
			    Arrays.asList("config" + File.separator + "pub_cs_list"));
		
		for(String cs_file: csListFiles) {
			export_args[0] = RETRY_ATTEMPTS;
			export_args[1] = READ_TIMEOUT;
			export_args[2] = CONNECTION_TIMEOUT;
			export_args[3] = DATABASE_URL;
			export_args[4] = DATABASE_PWD;
			export_args[5] = DATABASE_PWD;
			export_args[6] = CONTENT_TYPE;
			export_args[7] = "ContentSetList";
			export_args[8] = cs_file;
			//export_args[9] = "file:/data/tdm/newcontent_202108";		//file:/data/tdm/input/academicus
			export_args[9] = "file://input/newcontent_202107";	
			export_args[10] = "NA";
			export_args[11] = "NA";
			export_args[12] = "03/01/2021";
			export_args[13] = "08/01/2021";
		}
		
		
		List<String> params = new ArrayList<String>();
		// Java exe and parameters
		params.addAll(ExpandStrings(straJavaArgs));
		
		//params.addAll(ExpandStrings(classPath));
		params.add(System.getProperty("java.class.path"));
		
		// Common VM arguments
		params.addAll(Arrays.asList(export_args));
		
		ProcessBuilder processBuilder = new ProcessBuilder(params);
		processBuilder.redirectErrorStream(true);
		processBuilder.directory(new File(System.getProperty("user.dir")));
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HHmm");
		Calendar cal = Calendar.getInstance(  );
		String now = dateFormat.format(cal.getTime());

		File outputLog = new File( "logs" + File.separator + "ExportAUsFromArchive_" + now + ".log");
		File errorLog = new File( "logs" + File.separator + "ExportAUsFromArchive__error.log");

		processBuilder.redirectOutput(outputLog);
		processBuilder.redirectError(Redirect.appendTo(errorLog));
		
		//print args
		List<String> args = processBuilder.command();
		for(String arg: args) {
			System.out.println(arg);
		}
		
		try {
			Process process = processBuilder.start();
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	protected ArrayList<String> ExpandStrings(String[] stra)	{
		

		
		ArrayList<String> alResult = new ArrayList<String>();
		for (int i = 0; i < stra.length; i++)
		{
			alResult.add(stra[i]);
		}
		return alResult;
	}

	private void test1() {
		
		String current = "";
		try {
			current = new java.io.File( "." ).getCanonicalPath();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("Current dir:"+current);
        String currentDir = System.getProperty("user.dir");
        System.out.println("Current dir using System:" +currentDir);
		
		ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
        	System.out.println(url.getFile());
        }
        
        System.out.println(System.getProperty("java.class.path"));
        
	}
	
	
	private static void inheritIO(final InputStream src, final PrintStream dest) {
	    new Thread(new Runnable() {
	        public void run() {
	            Scanner sc = new Scanner(src);
	            while (sc.hasNextLine()) {
	                dest.println(sc.nextLine());
	            }
	        }
	    }).start();
	}

	
	

}


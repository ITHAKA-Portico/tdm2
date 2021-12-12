package org.portico.tdm.tdm2.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import org.apache.poi.util.StringUtil;

public class WebPageReader {

	String pageUrl;
	String pattern;
	List<String> contentLines;
	String redirectedUrl = null;
	
	public WebPageReader() {
		
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * Read a page URL into global variable contentLines
	 * @param rdf_url
	 * @throws IOException
	 */
	public void readUrl(String aPageUrl) throws IOException {
		
		setPageUrl(aPageUrl);
		
		List<String> content = new ArrayList<>();
		
		URL url;
	    InputStream is = null;
	    BufferedReader br;
	    String line;

	    try {

	        //Scenario 3: from java tutorial  https://docs.oracle.com/javase/tutorial/deployment/doingMoreWithRIA/accessingCookies.html
	        CookieManager manager = new CookieManager();
	        manager.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
	        CookieHandler.setDefault(manager);
	        url = new URL(getPageUrl());
	        if ( getPageUrl().startsWith("https://")) {
	        	HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
		        connection.addRequestProperty("User-Agent", "Portico Issue Checker");
		        //print cookies
		        CookieStore cookieJar = manager.getCookieStore();  // get cookies from underlying CookieStore
		        List<HttpCookie> cookies = cookieJar.getCookies();
		        for (HttpCookie cookie: cookies) {
		        	System.out.println("CookieHandler retrieved cookie: " + cookie);
		        }

		        is = connection.getInputStream();
		        if( !getPageUrl().equalsIgnoreCase(connection.getURL().toString())) {
		        	setRedirectedUrl(connection.getURL().toString());
		        }
	        }
	        else {
	        	URLConnection connection = url.openConnection();
	        	connection.addRequestProperty("User-Agent", "Portico Issue Checker");
	        	//print cookies
	        	CookieStore cookieJar = manager.getCookieStore();  // get cookies from underlying CookieStore
	        	List<HttpCookie> cookies = cookieJar.getCookies();
	        	for (HttpCookie cookie: cookies) {
	        		System.out.println("CookieHandler retrieved cookie: " + cookie);
	        	}

	        	is = connection.getInputStream();
	        	if( !getPageUrl().equalsIgnoreCase(connection.getURL().toString())) {
		        	setRedirectedUrl(connection.getURL().toString());
		        }
	        }
	        br = new BufferedReader(new InputStreamReader(is));

	        while ((line = br.readLine()) != null) {
	        	//System.out.println("line " + i++ + "------>" +  line);
 	        	content.add(line);

	        }
	        
	        //System.out.println("Totoal number of pattern has been found:" + getCount());
	    } catch (MalformedURLException mue) {
	         //mue.printStackTrace();
	    	throw mue;
	    } catch (IOException ioe) {
	        // ioe.printStackTrace();
	         System.out.println(ioe.getMessage());
	    	throw ioe;
	    } finally {
	        try {
	            if (is != null) is.close();
	        } catch (IOException ioe) {
	            
	        }
	    }
	    
	    setContentLines(content);
		
	}
	

	public String getTextFromUrl(String url) throws IOException {
		
		readUrl(url);

		String text = StringUtil.join("\n", getContentLines());
		
		return text;
		
	}



		
	public List<String> retrieveLineWithPattern(String patternToSearch) {
		
		if ( getContentLines() == null || getContentLines().isEmpty()) {
			return null;
		}
		
		List<String> matchedLines = new ArrayList<>();
		
		for(String line: getContentLines()) {
			if ( line.matches(".*" + patternToSearch + ".*")) {
				matchedLines.add(line);
			}
		}

		return matchedLines;
	}


	public String getPageUrl() {
		return pageUrl;
	}

	public void setPageUrl(String url) {
		this.pageUrl = url;
	}

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}


	public List<String> getContentLines() {
		return contentLines;
	}


	public void setContentLines(List<String> contentLines) {
		this.contentLines = contentLines;
	}


	public String getRedirectedUrl() {
		return redirectedUrl;
	}


	public void setRedirectedUrl(String redirectedUrl) {
		this.redirectedUrl = redirectedUrl;
	}


}

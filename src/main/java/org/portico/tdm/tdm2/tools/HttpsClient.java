package org.portico.tdm.tdm2.tools;

import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.Certificate;
import java.util.List;
import java.io.*;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

public class HttpsClient{
	
   public static void main(String[] args)
   {
        new HttpsClient().testIt();
   }
	
   private void testIt(){

      //String https_url = "https://insights.ovid.com/surgical-laparoscopy-endoscopy-percutaneous/slept/2011/02/000";
      //String https_url = "https://onlinelibrary.wiley.com/toc/20592310/91/3";
	  String https_url = "https://www.cambridge.org/core/product/identifier/AFR_83_2/type/JOURNAL_ISSUE";	  
    		  
      URL url;
      try {

	     url = new URL(https_url);

	     HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
	        connection.addRequestProperty("User-Agent", "Portico Issue Checker");
	        
	        
			/*connection.setRequestMethod("HEAD");         
			int code = connection.getResponseCode(); 
			System.out.println("Responde code: " + code);*/
			
			 CookieManager manager = new CookieManager();
		        manager.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
		        CookieHandler.setDefault(manager);
		        CookieStore cookieJar = manager.getCookieStore();  // get cookies from underlying CookieStore
		        List<HttpCookie> cookies = cookieJar.getCookies();
		        for (HttpCookie cookie: cookies) {
		        	System.out.println("CookieHandler retrieved cookie: " + cookie);
		        }

		       
			    
			
	     //dumpl all cert info
	    /* print_https_cert(con);
			
	     //dump all the content
	     print_content(con);*/
	     
	     InputStream is = connection.getInputStream();
	        InputStreamReader isr = new InputStreamReader(is);
	        BufferedReader br = new BufferedReader(isr);

	        String line;

	        String content = "";
	        int i=0;
	    	while ((line = br.readLine()) != null) {
	    		content += line;
	    		System.out.println("line " + i++ + "------>" +  line);

	    	}

	        br.close();
			
      } catch (MalformedURLException e) {
	     e.printStackTrace();
      } catch (IOException e) {
	     e.printStackTrace();
      }

   }
	
   private void print_https_cert(HttpsURLConnection con){
     
    if(con!=null){
			
      try {
				
	System.out.println("Response Code : " + con.getResponseCode());
	System.out.println("Cipher Suite : " + con.getCipherSuite());
	System.out.println("\n");
				
	Certificate[] certs = con.getServerCertificates();
	for(Certificate cert : certs){
	   System.out.println("Cert Type : " + cert.getType());
	   System.out.println("Cert Hash Code : " + cert.hashCode());
	   System.out.println("Cert Public Key Algorithm : " 
                                    + cert.getPublicKey().getAlgorithm());
	   System.out.println("Cert Public Key Format : " 
                                    + cert.getPublicKey().getFormat());
	   System.out.println("\n");
	}
				
	} catch (SSLPeerUnverifiedException e) {
		e.printStackTrace();
	} catch (IOException e){
		e.printStackTrace();
	}

     }
	
   }
	
   private void print_content(HttpsURLConnection con){
	if(con!=null){
			
	try {
		
	   System.out.println("****** Content of the URL ********");			
	   BufferedReader br = 
		new BufferedReader(
			new InputStreamReader(con.getInputStream()));
				
	   String input;
				
	   while ((input = br.readLine()) != null){
	      System.out.println(input);
	   }
	   br.close();
				
	} catch (IOException e) {
	   e.printStackTrace();
	}
			
       }
		
   }
	
}
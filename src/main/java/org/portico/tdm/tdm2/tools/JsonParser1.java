package org.portico.tdm.tdm2.tools;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonParser1 {

	public static void main(String[] args) {
		JsonParser1 parser = new JsonParser1();
		
		//parser.searchOneWordInFile("text1.json", "biology");
		
		parser.searchOneWorldInDir( "sampledata", "biology");

	}
	
	private int searchOneWorldInDir(String dirName, String aWord) {
		File[] files = new File( "input" + File.separator + dirName ).listFiles(
				new FilenameFilter() { @Override public boolean accept(File dir, String name) 
				      { return name.endsWith(".json"); } });
		int count = 0;
		
		for(File file: files) {
			String filename = file.getName();
			try {
				count += searchOneWordInFile( filename, aWord);
			}
			catch(Exception e) {
				System.out.println(filename + " " +e.getMessage());
			}
			
		}
		
		System.out.println( aWord  + " appeared " + count + " times in all json files under " + dirName);
		
		return count;
		
	}

	public int searchOneWordInFile(String filename, String aWord) {
		
		
		if ( aWord == null || aWord.isEmpty()) {
			return 0;
		}
		
		//read json file data to String
		byte[] jsonData = null;
		try {
			jsonData = Files.readAllBytes(Paths.get("input\\sampledata\\" + filename));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//create ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();
		//objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);	//doesn't work


		//read JSON like DOM Parser
		JsonNode rootNode = null;
		try {
			rootNode = objectMapper.readTree(jsonData);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		JsonNode idNode = rootNode.path("id");
		System.out.println("id = "+idNode.asText());


		//haven't found a way to do case insensitive searching
		List<JsonNode> bio_nodes = rootNode.findValues( aWord.toUpperCase());
		bio_nodes.addAll( rootNode.findValues( aWord.toLowerCase()));
		bio_nodes.addAll( rootNode.findValues( StringUtils.capitalize(aWord)));

		int count=0;
		for(JsonNode bio_node: bio_nodes){

			System.out.println( aWord + " object "+ bio_node.toString());

			Iterator<JsonNode> iterator = bio_node.elements();
			System.out.print( aWord + ": [ ");

			while (iterator.hasNext()) {
				JsonNode onePos = iterator.next();
				System.out.print(onePos.intValue() + "+"); 
				count+=onePos.intValue();
			}
			System.out.println("]");


		}
		System.out.println( aWord + " appeared " + count + " times in " + filename);

		return count;
		
	}

}

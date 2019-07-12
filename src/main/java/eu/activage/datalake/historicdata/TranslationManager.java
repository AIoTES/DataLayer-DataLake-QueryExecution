package eu.activage.datalake.historicdata;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TranslationManager {
	
	/*
	 * This class emulates the translation services of the (future) SIL v2
	 * 
	 * Syntactic translation available for universAAL, Fiware and SOFIA2
	 * 
	 * Input needed for syntactic translation:
	 * - Data from the platform (same format as real-time measurements for historic data)
	 * - Platform type identifier (same values as in the bridges)
	 * 
	 * Semantic translation using IPSM and semantic alignments (semantic alignment name and version provided by the historic data service emulator)
	 * 
	 * */
	
	private final Logger logger = LoggerFactory.getLogger(TranslationManager.class);
	
	private String ipsmUrl; // http://localhost:8888/
	private String platformId;
	String serviceRegistryUrl;
	final String DB = "services";
	final String IPSM_TYPE = "semantic-translator";
	final String SYNTACTIC_TYPE = "syntactic-translator";
	
	public TranslationManager(String url){
		// Get IPSM URL from registry
		serviceRegistryUrl = url + DB;
		ipsmUrl = null;
		platformId = null;
	}
	
	public TranslationManager(){
		Properties prop = new Properties();
		InputStream input = null;
		serviceRegistryUrl = null;
		try {
			input = getClass().getClassLoader().getResourceAsStream("config.properties");
			// load a properties file
			prop.load(input);
			ipsmUrl = prop.getProperty("ipsm-url");
			if (!ipsmUrl.endsWith("/")) ipsmUrl = ipsmUrl + "/";
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
    		if (input != null) {
    			try {
    				input.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
		platformId = "";
	}
	
	void setPlatformId(String id) throws Exception{
		platformId = id;
		ipsmUrl = getIpsmUrl();
	}
	
	// SEMANTIC TRANSLATION METHODS
	
	public String semanticTranslation(String data, String alignName, String alignVersion) throws Exception{
		String result = data;
		HttpClient httpClient = HttpClientBuilder.create().build();
		if(ipsmUrl!=null && !ipsmUrl.equals("")){
		   // Call IPSM for semantic translation
		   HttpPost ipsmPost = new HttpPost(ipsmUrl + "translation");
		   JsonObject translationData = new JsonObject();
		   JsonObject alignId = new JsonObject(); 
		   alignId.addProperty("name", alignName);
		   alignId.addProperty("version", alignVersion);
		   JsonArray array = new JsonArray();
		   array.add(alignId);
		   translationData.add("alignIDs", array);
		   translationData.addProperty("graphStr", result);
		   HttpEntity translationEntity = new StringEntity(translationData.toString(), ContentType.APPLICATION_JSON);
		   ipsmPost.setEntity(translationEntity);
		   HttpResponse httpResponse = httpClient.execute(ipsmPost);
		   int responseCode = httpResponse.getStatusLine().getStatusCode();
		   if(responseCode==200){
			   HttpEntity responseEntity = httpResponse.getEntity();
			   if(responseEntity!=null) {
				   JsonParser parser = new JsonParser();
				   JsonObject responseBody = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonObject();
				   result = responseBody.get("graphStr").getAsString();
			   }
		   }else{
				throw new Exception("Response code received from IPSM: " + responseCode);
		   }
		}else{
		   logger.warn("Could not send data to IPSM: no URL. No semantic translation was performed.");
		}
		return result;
	}
		
	private String getIpsmUrl() throws Exception{
		String url = null;	
		if(serviceRegistryUrl!=null){
			// Use JSON server
			HttpClient httpClient = HttpClientBuilder.create().build();
			URI requestUrl = new URIBuilder(serviceRegistryUrl).addParameter("type", IPSM_TYPE).build();
			HttpGet httpGet = new HttpGet(requestUrl);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			int responseCode = httpResponse.getStatusLine().getStatusCode();
			   if(responseCode==200){
				   HttpEntity responseEntity = httpResponse.getEntity();
				   if(responseEntity!=null) {
					   JsonParser parser = new JsonParser();
					   JsonObject target = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonArray().get(0).getAsJsonObject();
					   url = target.get("url").getAsString();
					   if (!url.endsWith("/")) url = url + "/";
				   }else {
					   logger.warn("No semantic translation service found.");
					   url = ipsmUrl; // null
				   }
			   }else{
				   throw new Exception("Response code received from Registry: " + responseCode);
			   }
		}else{
			url = ipsmUrl;
		}
		return url;
	}
	
	
	// SYNTACTIC TRANSLATION METHODS
	
	public String syntacticTranslation(String data, String type) throws Exception{
		String response = null;
		// SELECT SYNTACTIC TRANSLATOR USING THE PLATFORM TYPE IDENTIFIER (SAME VALUE AS IN THE BRIDGE)
		// get syntactic translation web services URLs from the registry
		String url = getSytacticTranslatorUrl(type);
		if(url != null){
			// Call syntactic translation web service
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpPost httpPost = new HttpPost( url + "/translate");
			HttpEntity requestEntity = new StringEntity(data, ContentType.APPLICATION_JSON);
			httpPost.setEntity(requestEntity);
			HttpResponse httpResponse = httpClient.execute(httpPost);
			HttpEntity responseEntity = httpResponse.getEntity();
			int responseCode = httpResponse.getStatusLine().getStatusCode();
			logger.info("Response code: " + httpResponse.getStatusLine().getStatusCode());
			if(responseCode==200){
				if(responseEntity!=null) {
					response = EntityUtils.toString(responseEntity);
				}
			}else{
				throw new Exception("Could translate data. Response code received from syntactic translation web service: " + responseCode);
			}			
		}else{
			throw new Exception("No syntactic translation service found for platform type " + type);
		}
		return response;
	}
		
	private String getSytacticTranslatorUrl(String type) throws Exception{
		String url = null;	
		if(serviceRegistryUrl!=null){
			// Use JSON server
			HttpClient httpClient = HttpClientBuilder.create().build();
			URI requestUrl = new URIBuilder(serviceRegistryUrl).addParameter("type", SYNTACTIC_TYPE).addParameter("platformType", type).build();
			HttpGet httpGet = new HttpGet(requestUrl);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			int responseCode = httpResponse.getStatusLine().getStatusCode();
			   if(responseCode==200){
				   HttpEntity responseEntity = httpResponse.getEntity();
				   if(responseEntity!=null) {
					   JsonParser parser = new JsonParser();
					   JsonObject target = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonArray().get(0).getAsJsonObject();
					   url = target.get("url").getAsString();
				   }else {
					   throw new Exception("No syntactic translation service found for platform type " + type);
					 //  logger.info("No syntactic translation service found for platform type " + type + ". Old syntactic tranlation methods will be used instead.");
				   }
			   }else{
				   throw new Exception("Response code received from Registry: " + responseCode);
			   }
		}else{
			// TODO: throw exception. For the moment, use old syntactic translation methods.
			logger.info("Service Registry not found. Old syntactic tranlation methods will be used instead.");
		}
		return url;
	}
	
}

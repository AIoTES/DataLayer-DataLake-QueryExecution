package eu.activage.datalake.historicdata;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DatabaseManager {
	
	JsonArray dbs;
	String serviceRegistryUrl;
	String platformRegistryUrl;
	final String SERVICES = "services";
	final String PLATFORMS = "platforms";
	final String PLATFORM_WEBSERVICE = "platform-historic";
	final String INDEPENDENT_STORAGE = "independent-storage";
	final String ID = "id";
	final String TYPE = "type";
	final String URL = "url";
	final String SOURCES  = "sources";
	final String DS = "DS";
	final String PLATFORM = "platform";
	final String PLATFORM_ID = "platformId";
	final String PLATFORM_TYPE = "platformType";
	final String USER = "user";
	final String PASSWORD = "password";
	private final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);
	
	// The current implementation uses JSON Server to store the service management data
	
	public DatabaseManager(String url){
		// Use prototype service registry (JSON server)
		serviceRegistryUrl = url + SERVICES;
		platformRegistryUrl = url + PLATFORMS;
		dbs = null;
	}
	
	public DatabaseManager(){
		// Use mock database (in resources folder)
		serviceRegistryUrl = null;
		platformRegistryUrl = null;
		JsonParser parser = new JsonParser();
		// Get available databases
		URL url = Resources.getResource("databases.json");
	    try {
			String data = Resources.toString(url, Charsets.UTF_8);
			dbs = parser.parse(data).getAsJsonArray();
						
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public JsonObject getDb(String id) throws Exception{
		JsonObject target = null;
		
		if(serviceRegistryUrl!=null){
			// Use JSON server
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet(serviceRegistryUrl + "/" + id);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			int responseCode = httpResponse.getStatusLine().getStatusCode();
			   if(responseCode==200){
				   HttpEntity responseEntity = httpResponse.getEntity();
				   if(responseEntity!=null) {
					   JsonParser parser = new JsonParser();
					   target = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonObject();
				   }else target = null; 
			   }else if (responseCode==404){
				   throw new Exception("Service " + id + " not found in the registry"); // Specific error message for service not registered (404 code)
			   }else{
				   throw new Exception("Response code received from Registry: " + responseCode);
			   }
		}else{
			// Use mock registry
			for(int i=0; i<dbs.size(); i++){
				JsonObject db = dbs.get(i).getAsJsonObject();
				String elementId = db.get(ID).getAsString();
				if(id.equals(elementId)) target = db;
			}
		}
		return target;
	}
	
	public String[] getDBIds(String[] ds, String[] platform) throws Exception{
		JsonParser parser = new JsonParser();
		List<String> ids = new ArrayList<String>();
		JsonArray target = new JsonArray();
		
		// Use JSON server
		HttpClient httpClient = HttpClientBuilder.create().build();
		if (platform!=null && ds==null){
			// Get service Ids for each platform type
			for(String param:platform){
				URI requestUrl = new URIBuilder(serviceRegistryUrl).addParameter(PLATFORM, param).build();
				HttpGet httpGet = new HttpGet(requestUrl);
				HttpResponse httpResponse = httpClient.execute(httpGet);
				int responseCode = httpResponse.getStatusLine().getStatusCode();
				if(responseCode==200){
				   HttpEntity responseEntity = httpResponse.getEntity();
				   if(responseEntity!=null) {
					   JsonArray responseArray = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonArray();
					   if(responseArray.size() > 0)  target.addAll(responseArray);
					   else logger.error("Platform " + param + " not found in the registry"); 
				   } 
				}else{
					throw new Exception("Response code received from Registry: " + responseCode);
				}
			}
			
		} else if(platform == null && ds != null){
			// Get service Ids for each DS
			for(String param:ds){
				URI requestUrl = new URIBuilder(serviceRegistryUrl).addParameter(DS, param).build();
				HttpGet httpGet = new HttpGet(requestUrl);
				HttpResponse httpResponse = httpClient.execute(httpGet);
				int responseCode = httpResponse.getStatusLine().getStatusCode();
			    if(responseCode==200){
					HttpEntity responseEntity = httpResponse.getEntity();
					if(responseEntity!=null) {
						 JsonArray responseArray = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonArray();
						 if(responseArray.size() > 0) target.addAll(responseArray);
						 else logger.error("DS " + param + " not found in the registry");
					}  
			    }else{
					throw new Exception("Response code received from Registry: " + responseCode);
				}
			}
			
		} else{ 
			throw new Exception("Incorrect request parameters");
		}
		
		// TODO: check if response is empty
		
		for(int i=0;i<target.size();i++){
		    JsonObject object = target.get(i).getAsJsonObject();
			String type = object.get(TYPE).getAsString();
			// Check types
			if(type.equals(PLATFORM_WEBSERVICE) || type.equals(INDEPENDENT_STORAGE))
			 ids.add(object.get(ID).getAsString());
	   }
				
		return ids.toArray(new String[ids.size()]);
	}
	
	public JsonObject getPlatform(String platformId) throws Exception{
		JsonObject target = null;
		if(platformRegistryUrl!=null){
			// Use JSON server
			HttpClient httpClient = HttpClientBuilder.create().build();
			URIBuilder builder = new URIBuilder(platformRegistryUrl);
	  		builder.addParameter(ID, platformId);
	  		URI uri = builder.build();
	  		HttpGet httpGet = new HttpGet(uri);
			
			HttpResponse httpResponse = httpClient.execute(httpGet);
			int responseCode = httpResponse.getStatusLine().getStatusCode();
			   if(responseCode==200){
				   HttpEntity responseEntity = httpResponse.getEntity();
				   if(responseEntity!=null) {
					   JsonParser parser = new JsonParser();
					   target = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonArray().get(0).getAsJsonObject();
				   }else target = null; 
			   }else if (responseCode==404){
				   throw new Exception("Platform " + platformId + " not found in the registry"); // Specific error message for service not registered (404 code)
			   }else{
				   throw new Exception("Response code received from Registry: " + responseCode);
			   }
		}else{
			// Use mock registry
			for(int i=0; i<dbs.size(); i++){
				JsonObject db = dbs.get(i).getAsJsonObject();
				String elementId = db.get(PLATFORM_ID).getAsString();
				if(platformId.equals(elementId)) target = db;
			}
		}
		return target;
	}
	
	public boolean isIndependentDataStorage(String id) throws Exception{
		// check also if it's a historic data web service?
		JsonObject db = getDb(id);
		if(db != null){
			String type = db.get(TYPE).getAsString();
			if(type.equals(INDEPENDENT_STORAGE)) return true; 
			else return false;
		} 
		else throw new Exception("Data origin not found");
	}
	
	public String getUrl(String id) throws Exception{ // Get db location
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get(URL).getAsString();	
		return response;
	}
	
	public String getPlatformId(String id) throws Exception{ // To get translation data from the SIL registry
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get(SOURCES).getAsJsonArray().get(0).getAsString(); // TODO: perhaps it should return the full array (?)
		return response;
	}
	
	public String getPlatformType(String id) throws Exception{ // For syntactic translation
		String type = null;
		String platformId = getPlatformId(id);
		JsonObject db = getPlatform(platformId);
		if(db != null && !isIndependentDataStorage(id)) {
			type = db.get(PLATFORM_TYPE).getAsString();
		}
		return type;
	}
	
	public String[] getUptreamInputAlignment(String id) throws Exception{ // For semantic translation
		String[] alignment = null;
		String platformId = getPlatformId(id);
		JsonObject db = getPlatform(platformId);
		if(db != null && !isIndependentDataStorage(id)){
			alignment = new String[2];
			alignment[0] = db.get("upstreamInputAlignmentName").getAsString();
			alignment[1] = db.get("upstreamInputAlignmentVersion").getAsString();
		}	
		return alignment;
	}
	
	public String[] getUptreamOutputAlignment(String id) throws Exception{ // For semantic translation
		String[] alignment = null;
		String platformId = getPlatformId(id);
		JsonObject db = getPlatform(platformId);
		if(db != null && !isIndependentDataStorage(id)){
			alignment = new String[2];
			alignment[0] = db.get("upstreamOutputAlignmentName").getAsString();
			alignment[1] = db.get("upstreamOutputAlignmentVersion").getAsString();
		}	
		return alignment;
	}
	
	public String[] getDownstreamInputAlignment(String id) throws Exception{
		// Not needed for reading data
		String[] alignment = null;
		String platformId = getPlatformId(id);
		JsonObject db = getPlatform(platformId);
		if(db != null && !isIndependentDataStorage(id)){
			alignment = new String[2];
			alignment[0] = db.get("downstreamInputAlignmentName").getAsString();
			alignment[1] = db.get("downstreamInputAlignmentVersion").getAsString();
		}	
		return alignment;
	}
	
	public String[] getDownstreamOutputAlignment(String id) throws Exception{
		// Not needed for reading data
		String[] alignment = null;
		String platformId = getPlatformId(id);
		JsonObject db = getPlatform(platformId);
		if(db != null && !isIndependentDataStorage(id)){
			alignment = new String[2];
			alignment[0] = db.get("downstreamOutputAlignmentName").getAsString();
			alignment[1] = db.get("downstreamOutputAlignmentVersion").getAsString();
		}	
		return alignment;
	}
	
	public String getUser(String id) throws Exception{
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get(USER).getAsString();	
		return response;		
	}
	
	public String getPassword(String id) throws Exception{
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get(PASSWORD).getAsString();	
		return response;		
	}
	
	public String[] getIdsUrl() throws Exception{
		// Get URLs of the registered Independent Data Storage instances
		List<String> response = new ArrayList<String>();
		if(serviceRegistryUrl!=null){
			// Use JSON server
			HttpClient httpClient = HttpClientBuilder.create().build();
			URI requestUrl = new URIBuilder(serviceRegistryUrl).addParameter(TYPE, INDEPENDENT_STORAGE).build();
			HttpGet httpGet = new HttpGet(requestUrl);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			int responseCode = httpResponse.getStatusLine().getStatusCode();
			   if(responseCode==200){
				   HttpEntity responseEntity = httpResponse.getEntity();
				   if(responseEntity!=null) {
					   JsonParser parser = new JsonParser();
					   JsonArray res = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonArray();
					   for(int i=0; i<res.size(); i++){
						   JsonObject target = res.get(i).getAsJsonObject();
						   String url = target.get(URL).getAsString();
						   // Return each URL once
						   if(!response.contains(url)) response.add(url);
					   }
				   }
			   }else{
				   throw new Exception("Response code received from Registry: " + responseCode);
			   }
		}else{
			throw new Exception("No service registry");
		}
		return response.toArray(new String[response.size()]);
	}
	
}

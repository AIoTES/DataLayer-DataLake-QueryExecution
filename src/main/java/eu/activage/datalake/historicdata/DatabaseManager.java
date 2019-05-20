package eu.activage.datalake.historicdata;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DatabaseManager {
	
	JsonArray dbs;
	String serviceRegistryUrl;
	String platformRegistryUrl;
	final String DB = "historic-webservices";
	final String SERVICES = "services";
	final String PLATFORMS = "platforms";
	
	// TODO: USE A REAL DATABASE
	// TODO: ADD MANAGEMENT FUNCTIONS (REGISTER, UNREGISTER...)?
	
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
				String elementId = db.get("id").getAsString();
				if(id.equals(elementId)) target = db;
			}
		}
		return target;
	}
	
	public JsonObject getPlatform(String platformId) throws Exception{
		JsonObject target = null;
		
		if(platformRegistryUrl!=null){
			// Use JSON server
			HttpClient httpClient = HttpClientBuilder.create().build();
			URIBuilder builder = new URIBuilder(platformRegistryUrl);
	  		builder.addParameter("id", platformId);
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
				String elementId = db.get("platformId").getAsString();
				if(platformId.equals(elementId)) target = db;
			}
		}
		return target;
	}
	
	public boolean isIndependentDataStorage(String id) throws Exception{
		// check also if it's a historic data web service?
		JsonObject db = getDb(id);
		if(db != null){
			String type = db.get("type").getAsString();
			if(type.equals("independent-storage")) return true; // "common"
			else return false;
		} 
		else throw new Exception("Data origin not found");
	}
	
	public String getUrl(String id) throws Exception{ // Get db location
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get("url").getAsString();	
		return response;
	}
	
	public String getPlatformId(String id) throws Exception{ // To get data from the SIL registry
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get("platforms").getAsJsonArray().get(0).getAsString();
		return response;
	}
	
	public String getPlatformType(String id) throws Exception{ // For syntactic translation
		String type = null;
		String platformId = getPlatformId(id);
		JsonObject db = getPlatform(platformId);
		if(db != null && !isIndependentDataStorage(id)) {
			type = db.get("platformType").getAsString();
		}
		return type;
	}
	
	public String[] getUptreamAlignment(String id) throws Exception{ // For semantic translation
		//TODO: A channel may have two alignments. There should be an input and an ouput alignment in the registry
		String[] alignment = null;
		String platformId = getPlatformId(id);
		JsonObject db = getPlatform(platformId);
		if(db != null && !isIndependentDataStorage(id)){
			alignment = new String[2];
			alignment[0] = db.get("upstreamAlignmentName").getAsString();
			alignment[1] = db.get("upstreamAlignmentVersion").getAsString();
		}	
		return alignment;
	}
	
	public String[] getDownstreamAlignment(String id) throws Exception{
		// Not needed for reading data
		//TODO: A channel may have two alignments. There should be an input and an ouput alignment in the registry
		String[] alignment = null;
		String platformId = getPlatformId(id);
		JsonObject db = getPlatform(platformId);
		if(db != null && !isIndependentDataStorage(id)){
			alignment = new String[2];
			alignment[0] = db.get("downstreamAlignmentName").getAsString();
			alignment[1] = db.get("downstreamAlignmentVersion").getAsString();
		}	
		return alignment;
	}
	
	public String getUser(String id) throws Exception{
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get("user").getAsString();	
		return response;		
	}
	
	public String getPassword(String id) throws Exception{
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get("password").getAsString();	
		return response;		
	}
	
}

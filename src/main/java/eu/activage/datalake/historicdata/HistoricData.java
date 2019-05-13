package eu.activage.datalake.historicdata;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class HistoricData {
	
	/*
	 * This class emulates the historic data service of the (future) SIL v2
	 * 
	 * The method getFromPlatform identifies the proper web service, gets the data and calls the translation services
	 * The information needed to locate the web services and perform the translations is stored in a database (will need management operations)
	 * 
	 * */
	
    private final Logger logger = LoggerFactory.getLogger(HistoricData.class);
    private final String IDS_COLUMN = "observation";
    
    TranslationManager manager;
    DatabaseManager dbManager;
    
    public HistoricData(String url){
    	// Use service registry prototype
    	manager = new TranslationManager(url);
    	dbManager = new DatabaseManager(url);
    }
    
    public HistoricData(){
    	// Use mock service registry
    	manager = new TranslationManager();
    	dbManager = new DatabaseManager();
    }
      
    public JsonArray getData(String platform, String deviceId, String deviceType, String fromDate, String toDate) throws Exception{
    	/*
         * Create and execute call to web service
         * */
		JsonArray response = null;
		
		logger.info("Retrieving historic data from platform: " + platform);
		
		if(dbManager.isIndependentDataStorage(platform)){
			String query = createIdsQuery(deviceType, deviceId, fromDate, toDate); // Table = deviceType
			response = getFromIds(platform, deviceType, query); // Table = deviceType
		}else{
			response = getFromPlatform(platform, deviceId, deviceType, fromDate, toDate);
		}
								
		return response;
	}
    
    public JsonObject getWebserviceCall(String platform, String deviceId, String deviceType, String fromDate, String toDate) throws Exception{
    	/*
    	 * Returns the complete URL and headers to call the web service using GET
    	 * */
    	URI responseUri = null;
    	String url = dbManager.getUrl(platform);
    	
    	if(dbManager.isIndependentDataStorage(platform)){
    		responseUri = createIdsCall(url, platform, deviceId, deviceType, fromDate, toDate);
		}else{
			responseUri = createUri(url, deviceId, deviceType, fromDate, toDate);
		}
    	JsonObject response = new JsonObject();
    	response.addProperty("url", responseUri.toString());
    	JsonObject headers = new JsonObject();
    	
    	String user = dbManager.getUser(platform);
    	String password = dbManager.getPassword(platform);
    	
    	if(user!=null && !user.equals("") && password!=null && !password.equals("")){
    		String authString = user + ":" + password;
    		byte[] authEncBytes = Base64.getEncoder().encode(authString.getBytes("UTF-8"));
    		String authStringEnc = new String(authEncBytes);
    		String authHeader = "Basic " + authStringEnc;
    		headers.addProperty("Authentication", authHeader);
    	}
    	response.add("headers", headers);
    	
    	return response;
    }
    
    public JsonArray getFromPlatform(String id, String deviceId, String deviceType, String dateFrom, String dateTo) throws Exception{
    	// Call web service and get data in the platform's format
    	JsonArray result = null;
    	JsonParser parser = new JsonParser();
    	
    	// URL of the webservice
    	String url = dbManager.getUrl(id);
    	
    	// For translation
    	String platformType = dbManager.getPlatformType(id);
    	String[] alignment = dbManager.getUptreamAlignment(id);
    	
    	// Authentication
    	String user = dbManager.getUser(id);
    	String password = dbManager.getPassword(id);
    	
    	// Get historic data from the webservice
    	URI uri = createUri(url, deviceId, deviceType, dateFrom, dateTo);
    	String resultRaw = callWebService(uri, user, password);
    	    	
    	// Translation of each individual message
 	    if(!resultRaw.isEmpty()){
 		   if(platformType!=null && !platformType.equals("")){
 			   // Syntactic translation
 			   JsonArray input = parser.parse(resultRaw).getAsJsonArray();
 			   JsonArray output = new JsonArray();
 			   
 			   logger.info("Retrieved " + input.size() + " measurements from " + id);
 			   logger.info("Platform type: " + platformType);
 			   if(alignment!=null && !alignment[0].isEmpty() && !alignment[1].isEmpty()) logger.info("Semantic translation using " + alignment[0] + " alignment.");
 			   logger.info("Processing data...");
 			   manager.setPlatformId(dbManager.getPlatformId(id));
 			   for(int i=0; i<input.size(); i++){
 				   String translatedData = null;
 				   // Syntactic translation
 				   String observation = manager.syntacticTranslation(input.get(i).getAsString(), platformType);
 				   
 				   // Semantic translation
 			 	   if(observation!=null) {
 					   if(alignment!=null && !alignment[0].isEmpty() && !alignment[1].isEmpty())
 						  translatedData = manager.semanticTranslation(observation, alignment[0], alignment[1]);
 					   else translatedData = observation;
 				   }
 				   
 				   output.add(parser.parse(translatedData).getAsJsonObject());
 			   }
 			  logger.info("Success");
 			   result = output;   
 		   }else{
 			   // Return data in universAAL format
 			   logger.info("Sending response without translation...");
 			   result = parser.parse(resultRaw).getAsJsonArray();
 		   }
 	   }     
    	
    	return result;
    }
    
    URI createUri(String url, String deviceId, String deviceType, String dateFrom, String dateTo) throws Exception{
    	URI uri = null;
    	// Create URL for the web service call
  	   
  	   if(url!=null && !url.isEmpty()){
  		   // TODO: define standard interface
  		   
//  		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd"); // Format of the input query date values. No time information included
  		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Format of the input query date values. No time information included
  		   Date startDate = f.parse(dateFrom);
  		   Date endDate = f.parse(dateTo);
//  		   f.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Example: 2018-02-01T00:00:00.000Z
  		   
  		   URIBuilder builder = new URIBuilder(url);
  		   if(deviceType!=null) builder.addParameter("deviceType", deviceType);
  		   if(deviceId!=null) builder.addParameter("deviceId", deviceId);
  		   if(startDate!=null) builder.addParameter("startDate", f.format(startDate) + "Z");
  		   if(endDate!=null) builder.addParameter("endDate", f.format(endDate) + "Z");
  		   uri = builder.build();
  	   }
  	   
  	   return uri;
    }
    
    String callWebService(URI uri, String name, String password) throws Exception{
    	// Retrieve data from web service
 	   String resultRaw = "";
 	    	   
 	   if(uri!=null){
 		   // Call web service and get data in the platform's format
 		   
 		   // Basic authentication
 		   String authString = name + ":" + password;
 		   byte[] authEncBytes = Base64.getEncoder().encode(authString.getBytes("UTF-8"));
 		   String authStringEnc = new String(authEncBytes);
 		   String authHeader = "Basic " + authStringEnc;
 		   
 		  HttpURLConnection con = (HttpURLConnection) uri.toURL().openConnection();

 			// optional default is GET
 		 con.setRequestMethod("GET");

 			//add request header
 		 con.setRequestProperty("User-Agent", "Mozilla/5.0");
 		 con.setRequestProperty("Authorization",  authHeader);
 			
 		 int responseCode = con.getResponseCode();
 		 System.out.println("\nSending 'GET' request to URL : " + uri.toURL());
 		 System.out.println("Response Code : " + responseCode);

 		 if(responseCode>=200 && responseCode<=299){
 			BufferedReader in = new BufferedReader(
 	 		        new InputStreamReader(con.getInputStream()));
 	 		String inputLine;
 	 		StringBuffer response = new StringBuffer();
 	 		while ((inputLine = in.readLine()) != null) {
 	 			response.append(inputLine);
 	 		}
 	 		in.close();
 	 			
 	 		resultRaw = response.toString();
 		 }else{
 			throw new Exception("Unsuccessful response code from server: " + responseCode);
 		 }
 			
 	   }else{
 		  throw new Exception("No web service found for the requested data");
 	   }
 	   return resultRaw;
    }
    
   public JsonArray getFromIds(String db, String table, String query) throws Exception{
    	/*
    	 * Get data from the Independent Data Storage
    	 * 
    	 * */
		// The db name in the Independent Data Storage is used as identifier in the DB registry
		JsonArray response = null;
		JsonObject body = new JsonObject();
		body.addProperty("db", db);
		body.addProperty("table", table);
		body.addProperty("query", query);
		
		String url = dbManager.getUrl(db);
		if(!url.endsWith("/")) url = url + "/";
		
		HttpClient httpClient = HttpClientBuilder.create().build();
		
		HttpPost httpPost = new HttpPost( url + "independentStorage/select");
		
		 HttpEntity translationEntity = new StringEntity(body.toString(), ContentType.APPLICATION_JSON);
		 httpPost.setEntity(translationEntity);
		 HttpResponse httpResponse = httpClient.execute(httpPost);
		 HttpEntity responseEntity = httpResponse.getEntity();
		 int responseCode = httpResponse.getStatusLine().getStatusCode();
		 logger.info("Response code: " + httpResponse.getStatusLine().getStatusCode());
		 if(responseCode==200){
			 if(responseEntity!=null) {
				 JsonParser parser = new JsonParser();
				 String data = EntityUtils.toString(responseEntity);
				 JsonArray array = parser.parse(data).getAsJsonArray();
				 response = new JsonArray();
				 for(JsonElement observation:array){
					 String meas = observation.getAsJsonObject().get(IDS_COLUMN).getAsString();
					 response.add(parser.parse(meas).getAsJsonObject());
				 }
			 }
		 }else{
				throw new Exception("Could not retrieve data. Response code received from Independent Data Storage: " + responseCode);
		 }	
		return response;
	}
   
   String createIdsQuery(String table, String deviceId, String fromDate, String toDate){
	   /*
	    * Construct query for Independent Data Storage
	    * */
	   
	   String q = "SELECT * ";
//	   String q = "SELECT " + IDS_COLUMN;
		q = q + " FROM \"" + table + "\"";
	   // WHERE
		if(deviceId!=null || fromDate!=null || toDate!=null){
			q = q + " WHERE ";
			if(fromDate != null) q = q + "time >= '" +  fromDate + "'"; // String value
			if(fromDate != null && toDate !=null) q = q + " AND ";
			if(toDate != null) q = q + "time <= '" +  toDate + "'"; // String value
			if((fromDate != null || toDate != null) && deviceId != null) q = q + " AND ";
			if(deviceId != null) q = q + "device = '" +  deviceId + "'";	// String value
		}
		logger.info(q); // Debug
	   return q;
   }
   
   URI createIdsCall(String url, String db, String deviceId, String deviceType, String dateFrom, String dateTo) throws Exception{
	   /*
	    * Create URL for GET method
	    * */
	   URI uri = null;
	   if(url!=null && !url.isEmpty()){
  		   if(!url.endsWith("/")) url = url + "/"; // Just in case
		   
//  		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
  		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Format of the input query date values. TODO: check and define input format
  		   Date startDate = f.parse(dateFrom);
  		   Date endDate = f.parse(dateTo);
//  		   f.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Example: 2018-02-01T00:00:00.000Z
  		   
  		   String query = createIdsQuery(deviceType, deviceId, startDate.toString(), endDate.toString()); // DB = deviceType
  		   String encodedQuery = URLEncoder.encode(query, "UTF-8");
  		   
  		   URIBuilder builder = new URIBuilder(url + "independentStorage/select");
  		   builder.addParameter("db", db);
  		   builder.addParameter("table", deviceType); // TODO: check if this is a good approach
  		   builder.addParameter("query", encodedQuery);
  		   uri = builder.build();
  	   }
  	   
  	   return uri;
	   
   }
}

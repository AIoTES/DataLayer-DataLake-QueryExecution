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
			// Table = deviceType
			if(deviceType == null){
				// If deviceType is empty, list all tables and send the same query to all of them.
				// get tables from Independent Data Storage
				JsonArray types = getIdsTables(platform);
				if(types.size() > 0){
					response = new JsonArray();
					for(int i=0; i<types.size(); i++){
						deviceType = types.get(i).getAsString();
						String query = createIdsQuery(deviceType, deviceId, fromDate, toDate);
						response.addAll(getFromIds(platform, deviceType, query));
					}
				}
				
			}else{
				// A device type was specified
				String query = createIdsQuery(deviceType, deviceId, fromDate, toDate); 
				response = getFromIds(platform, deviceType, query);
			}
		}else{
			response = getFromPlatform(platform, deviceId, deviceType, fromDate, toDate);
		}
								
		return response;
	}
    
    public JsonArray getWebserviceCall(String platform, String deviceId, String deviceType, String fromDate, String toDate) throws Exception{
    	/*
    	 * Returns the complete URL and headers to call the web service using GET
    	 * */
    	URI[] responseUri = null;
    	String url = dbManager.getUrl(platform);
    	JsonArray response = new JsonArray();
    	
    	if(dbManager.isIndependentDataStorage(platform)){
    		if(deviceType == null){
    			JsonArray types = getIdsTables(platform);
				if(types.size() > 0){
					responseUri = new URI[types.size()];
					for(int i=0; i<types.size(); i++){
						String type = types.get(i).getAsString();
						responseUri[i] = createIdsCall(url, platform, deviceId, type, fromDate, toDate);
					}
				}
    		}else{
    			responseUri = new URI[1];
    			responseUri[0] = createIdsCall(url, platform, deviceId, deviceType, fromDate, toDate);
    		}
		}else{
			responseUri = new URI[1];
			responseUri[0] = createUri(url, deviceId, deviceType, fromDate, toDate);
		}
    	
    	for(int i=0; i<responseUri.length; i++){
    		JsonObject responseObject = new JsonObject();
        	responseObject.addProperty("url", responseUri[i].toString());
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
        	responseObject.add("headers", headers);
        	response.add(responseObject);
    	}
    	return response;
    }
    
    public JsonArray getFromPlatform(String id, String deviceId, String deviceType, String dateFrom, String dateTo) throws Exception{
    	// Call web service and get data in the platform's format
    	JsonArray result = null;
    	JsonParser parser = new JsonParser();
    	
    	// URL of the web service
    	String url = dbManager.getUrl(id);
    	
    	// For translation
    	String platformType = dbManager.getPlatformType(id);
    	String[] inputAlignment = dbManager.getUptreamInputAlignment(id);
    	String[] outputAlignment = dbManager.getUptreamOutputAlignment(id);
    	
    	// Authentication
    	String user = dbManager.getUser(id);
    	String password = dbManager.getPassword(id);
    	
    	// Get historic data from the web service
    	URI uri = createUri(url, deviceId, deviceType, dateFrom, dateTo);
    	String resultRaw = callWebService(uri, user, password);
    	    	
    	// Translation of each individual message
 	    if(!resultRaw.isEmpty()){
 		   if(platformType!=null && !platformType.equals("")){
 			   // Syntactic translation (returns array of JSON-LD messages)
 			  String jsonldData = manager.syntacticTranslation(resultRaw, platformType);
 			   
// 			   JsonArray input = parser.parse(resultRaw).getAsJsonArray();
 			   JsonArray input = parser.parse(jsonldData).getAsJsonArray();
 			   JsonArray output = new JsonArray();
 			   
 			   logger.info("Retrieved " + input.size() + " measurements from " + id);
 			   logger.info("Platform type: " + platformType);
 			   logger.info("Processing data...");
 			   manager.setPlatformId(dbManager.getPlatformId(id));
 			   for(int i=0; i<input.size(); i++){
 				   String translatedData = null;
 				   String translatedData2 = null;
 				   
// 				   String observation = manager.syntacticTranslation(input.get(i).getAsString(), platformType);
 				   String observation = input.get(i).getAsJsonObject().toString();
 				   
 				   // Semantic translation
 			 	   if(observation!=null) {
 					   if(inputAlignment!=null && !inputAlignment[0].isEmpty() && !inputAlignment[1].isEmpty()){
 						   logger.info("Semantic translation using " + inputAlignment[0] + " alignment.");
 						   translatedData = manager.semanticTranslation(observation, inputAlignment[0], inputAlignment[1]);
 					   } else translatedData = observation;
 				   }
 			 	   if(translatedData!=null) {
					   if(outputAlignment!=null && !outputAlignment[0].isEmpty() && !outputAlignment[1].isEmpty()){
						   logger.info("Semantic translation using " + outputAlignment[0] + " alignment.");
						   translatedData2 = manager.semanticTranslation(translatedData, outputAlignment[0], outputAlignment[1]);
					   } else translatedData2 = translatedData;
				   }
 				   output.add(parser.parse(translatedData2).getAsJsonObject());
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
//  		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd"); // Format of the input query date values. No time information included
  		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Format of the input query date values. No time information included
//  		   f.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Example: 2018-02-01T00:00:00.000Z
  		   
  		   URIBuilder builder = new URIBuilder(url);
  		   if(deviceType!=null) builder.addParameter("deviceType", deviceType);
  		   if(deviceId!=null) builder.addParameter("deviceId", deviceId);
  		   if(dateFrom!=null){
  			 Date startDate = f.parse(dateFrom); 
  			 builder.addParameter("startDate", f.format(startDate) + "Z");
  		   } 
  		   if(dateTo!=null){
  			 Date endDate = f.parse(dateTo);
  			 builder.addParameter("endDate", f.format(endDate) + "Z");
  		   } 
  		   uri = builder.build();
  	   }
  	   return uri;
    }
    
    String callWebService(URI uri, String name, String password) throws Exception{
    	 // Call web service and get data in the platform's format
 	   String resultRaw = "";
 	    	   
 	   if(uri!=null){
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
    	 * The db name in the Independent Data Storage is used as identifier in the DB registry
    	 * */
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
					JsonObject meas = observation.getAsJsonObject().get(IDS_COLUMN).getAsJsonObject();
					response.add(meas);
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
	   String startDate = null;
	   String endDate = null;
	   if(url!=null && !url.isEmpty()){
  		   if(!url.endsWith("/")) url = url + "/"; // Just in case
//  		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
  		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Format of the input query date values
  		   if(dateFrom != null) startDate = f.parse(dateFrom).toString();
  		   if(dateTo != null) endDate = f.parse(dateTo).toString();
//  		   f.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Example: 2018-02-01T00:00:00.000Z
  		   String query = createIdsQuery(deviceType, deviceId, startDate, endDate); // DB = deviceType
  		   String encodedQuery = URLEncoder.encode(query, "UTF-8");
  		   
  		   URIBuilder builder = new URIBuilder(url + "independentStorage/select");
  		   builder.addParameter("db", db);
  		   builder.addParameter("table", deviceType);
  		   builder.addParameter("query", encodedQuery);
  		   uri = builder.build();
  	   }
  	   return uri;
   }
   
   public JsonArray getIdsTables(String db) throws Exception{
	   return getIdsTables(db, dbManager.getUrl(db));
   }
   
   JsonArray getIdsTables(String db, String url) throws Exception{
	   JsonArray response = null;
	   JsonObject body = new JsonObject();
	   body.addProperty("db", db);
	   if(!url.endsWith("/")) url = url + "/";
	   HttpClient httpClient = HttpClientBuilder.create().build();
	   HttpPost httpPost = new HttpPost( url + "independentStorage/tables");
	   HttpEntity requestEntity = new StringEntity(body.toString(), ContentType.APPLICATION_JSON);
	   httpPost.setEntity(requestEntity);
	   HttpResponse httpResponse = httpClient.execute(httpPost);
	   HttpEntity responseEntity = httpResponse.getEntity();
	   int responseCode = httpResponse.getStatusLine().getStatusCode();
	   logger.info("Response code: " + httpResponse.getStatusLine().getStatusCode());
	   if(responseCode==200){
			if(responseEntity!=null) {
				JsonParser parser = new JsonParser();
				String data = EntityUtils.toString(responseEntity);
				JsonObject responseObject = parser.parse(data).getAsJsonObject();
				if(!responseObject.entrySet().isEmpty()) response = responseObject.get("tables").getAsJsonArray();
				else response = new JsonArray();
			}
	   }else{
				throw new Exception("Could not retrieve DB names. Response code received from Independent Data Storage: " + responseCode);
	   }	
	   return response;
   }
   
   public JsonObject getIdsDbsAndTables() throws Exception{
	   JsonObject response = new JsonObject();
	   String[] urls = dbManager.getIdsUrl();
	   HttpClient httpClient = HttpClientBuilder.create().build();
	   for(String url:urls){
		   if(!url.endsWith("/")) url = url + "/";
		   HttpPost httpPost = new HttpPost( url + "independentStorage/databases");
		   HttpResponse httpResponse = httpClient.execute(httpPost);
		   HttpEntity responseEntity = httpResponse.getEntity();
		   int responseCode = httpResponse.getStatusLine().getStatusCode();
		   logger.info("Response code: " + httpResponse.getStatusLine().getStatusCode());
		   if(responseCode==200){
				if(responseEntity!=null) {
					JsonParser parser = new JsonParser();
					String data = EntityUtils.toString(responseEntity);
					JsonArray databases = parser.parse(data).getAsJsonObject().get("databases").getAsJsonArray();
					for(int i=0; i<databases.size(); i++){
						String dbName = databases.get(i).getAsString();
						// Remove _internal DB from results
						if(!dbName.equals("_internal")){
							JsonArray tables = getIdsTables(dbName, url);
							response.add(dbName, tables);
						}
					}
				}
		   }else{
					throw new Exception("Could not retrieve DB names. Response code received from Independent Data Storage: " + responseCode);
		   }	
	   }
	   return response;
   }
}

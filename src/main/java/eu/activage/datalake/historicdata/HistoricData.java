package eu.activage.datalake.historicdata;


import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;


public class HistoricData {
	
	/*
	 * This class emulates the historic data service of the (future) SIL v2
	 * 
	 * The method getFromPlatform identifies the proper webservice, gets the data and calls the translation services
	 * The information needed to locate the webservices and perform the translations is stored in a database (will need management operations)
	 * 
	 * */
	
    private final Logger logger = LoggerFactory.getLogger(HistoricData.class);
    
    TranslationManager manager;
    DatabaseManager dbManager;
    
    public HistoricData(){
    	manager = new TranslationManager();
    	dbManager = new DatabaseManager();
    }
    
    
    public String getFromPlatform(String id, String deviceId, String dateFrom, String dateTo) throws Exception{
    	String result = null;
    	JsonParser parser = new JsonParser();
    	
    	// URL of the webservice
    	String url = dbManager.getUrl(id);
    	
    	// For translation
    	String platformType = dbManager.getPlatformType(id);
    	String[] alignment = dbManager.getUptreamAlignment(id);
    	
    	
    	// Get historic data from the webservice
    	String resultRaw = callWebService(url, deviceId, dateFrom, dateTo);
    	    	
    	// Translation of each individual message
 	    if(!resultRaw.isEmpty()){
 		   if(platformType!=null && !platformType.equals("")){
 			   // Syntactic translation
 			   JsonArray input = parser.parse(resultRaw).getAsJsonArray();
 			   JsonArray output = new JsonArray();
 			   
 			   logger.info("Retrieved " + input.size() + " measurements.");
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
 				   
 				   output.add(translatedData);
 			   }
 			   result = output.toString();   
 		   }else{
 			   // Return data in universAAL format
 			   logger.info("Sending response without translation...");
 			   result = resultRaw;
 		   }
 	   }     
    	
    	return result;
    }
    
    String callWebService(String url, String deviceId, String dateFrom, String dateTo) throws Exception{
    	// Test with DS Greece webservice
 	   String resultRaw = "";
 	   String authToken = "a7e46008-b5ab-449c-8777-e39c4b30ed49"; // TODO: get all parameter from api call or properties
 	   String name = "admin";
 	   String password = "P@ssw0rd";
 	   
 	   // Device id should not be a numeric value. TODO: define standard interface for webservices
 	   // Temporary fix
 	   // 0 for motion sensor, 1 for door sensor and 2 for panic buttons
 	   switch(deviceId){
 	    case "motion":
 	    	deviceId="0";
 	    	break;
 	    case "door":
 	    	deviceId="1";
 	    	break;
 	    case "button":
 	    	deviceId="2";
 	    	break;
 	    default:
 	    	logger.info("Unrecognized device type");
 	   }
 	   
 	   
 	   if(url!=null && !url.isEmpty()){
 		   // Call webservice and get data in the platform's format
 		   // TODO: define standard interface
 		   
 		   SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd"); // Format of the input query date values. No time information included
 		   Date startDate = f.parse(dateFrom);
 		   Date endDate = f.parse(dateTo);
 		   f.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"); // Example: 2018-02-01T00:00:00.000Z
 		   
 		   // Basic authentication
 		   String authString = name + ":" + password;
 		   byte[] authEncBytes = Base64.getEncoder().encode(authString.getBytes());
 		   String authStringEnc = new String(authEncBytes);
 		   String authHeader = "Basic " + authStringEnc;
 		   System.out.println("Authentication header: " + authHeader);
 		   		   
 		   HttpClient httpClient = HttpClientBuilder.create().build();
 		   URI uri = new URIBuilder(url)
 				    .addParameter("deviceType", deviceId)  // 2. No deviceId, using this value as type
 				    .addParameter("startDate", f.format(startDate) + "Z") 
 				    .addParameter("endDate", f.format(endDate) + "Z")
 				    .addParameter("tenantAuthToken", authToken)
 				    .build();
 		   HttpGet httpGet = new HttpGet(uri);
 		   httpGet.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
 		   httpGet.addHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
 		   System.out.println(uri.toString());
 		   		   		   
 		   HttpResponse httpResponse = httpClient.execute(httpGet);
 		   // Do something with the response code?
 		   HttpEntity responseEntity = httpResponse.getEntity();
 		   if(responseEntity!=null) {
 			   resultRaw = responseEntity.toString();
 		   } 
 	   }else{
 		// Get test data from a file
 		   URL test = Resources.getResource("uaal-data.txt");
 	       resultRaw = Resources.toString(test, Charsets.UTF_8);
 	   }
 	   return resultRaw;
    }
    
}

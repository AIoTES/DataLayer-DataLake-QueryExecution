package eu.activage.datalake.historicdata;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import org.apache.http.client.utils.URIBuilder;
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
 				   
 				   output.add(translatedData);
 			   }
 			  logger.info("Success");
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
 		   byte[] authEncBytes = Base64.getEncoder().encode(authString.getBytes("UTF-8"));
 		   String authStringEnc = new String(authEncBytes);
 		   String authHeader = "Basic " + authStringEnc;
// 		   System.out.println("Authentication header: " + authHeader);
 		   
 		   URI uri = new URIBuilder(url)
 				    .addParameter("deviceType", deviceId)  // 2. No deviceId, using this value as type
 				    .addParameter("startDate", f.format(startDate) + "Z") 
 				    .addParameter("endDate", f.format(endDate) + "Z")
 				    .addParameter("tenantAuthToken", authToken)
 				    .build();
 		   
 		   // FIXME: connection issues from UPV if a proxy is not used
 		  System.setProperty("http.proxyHost", "158.42.247.100");
		  System.setProperty("http.proxyPort", "8080");
		  System.setProperty("https.proxyHost",  "158.42.247.100");
		  System.setProperty("https.proxyPort", "8080");
 		   
// 		  System.out.println(uri.toString()); 
 		   
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
 		// Get test data from a file
 		   URL test = Resources.getResource("uaal-data.txt");
 	       resultRaw = Resources.toString(test, Charsets.UTF_8);
 	   }
 	   return resultRaw;
    }
    
}

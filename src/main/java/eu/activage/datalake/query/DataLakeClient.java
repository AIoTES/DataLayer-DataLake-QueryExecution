package eu.activage.datalake.query;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.activage.datalake.historicdata.DatabaseManager;
import eu.activage.datalake.historicdata.HistoricData;

class DataLakeClient {
	
//	String historicUrl;
//	String storageUrl;
	// TODO: integrate with indexing service
	DatabaseManager dbManager;
	HistoricData historic = new HistoricData();
	
	private final Logger logger = LoggerFactory.getLogger(DataLakeClient.class);
	
//	DataLakeClient(String historicData, String independentData){
//		historicUrl = historicData;
//		storageUrl = independentData;
//		dbManager = new DatabaseManager();
//	}
	
	DataLakeClient(){
		dbManager = new DatabaseManager();
	}
	
	// Obtain location and type of the target database
	
	
	String execute(Query q) throws Exception{
		String response = null;
		
		String dbId = q.index;
		
		// Get database type and location
		String url = dbManager.getUrl(dbId); // TODO: this should be a part of the historic data service
			
		logger.info("Query parameterms: " + q.toString()); //DEBUG
		
		// TODO: analyze query and identify data sources (by the index) Table = index
		// In the current version, indices represent platforms (for the historic data module) and columns represent devices (or magnitudes?)
		
		// A simple call to the historic data service
//		response = getHistoricData(q.index, q.columns[0], q.conditions);
		
		// TODO: integrate indexing service
		if(dbManager.isIndependentDataStorage(dbId)){
			// Get data from the Independent Data Storage
			JsonArray res = new JsonArray();
			JsonParser parser = new JsonParser();
			for (int i = 0; i < q.columns.length; i++){
				System.out.println(q.createQueryString()); 
				String data = getFromIndependentStorage(q.index, q.columns[i], q.createQueryString()); // DB, table, query. TODO: check if this is the proper way to translate this call
				res.addAll(parser.parse(data).getAsJsonArray());
			
			}
			response = res.toString();
		}else{
			// Call historic data service for the target platform
			JsonArray res = new JsonArray();
			JsonParser parser = new JsonParser();
			for (int i = 0; i < q.columns.length; i++){
				String data = getHistoricData(q.index, q.columns[i], q.conditions); // TODO: change call
				res.addAll(parser.parse(data).getAsJsonArray());
			}
			response = res.toString();
		}
				
		return response;
	}	
	
	String getHistoricData(String platform, String device, String[] params) throws Exception{
		String response = "";
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); // Format of the input query date values. TODO: add time?
		// TODO: check date format of the historic data service
		
		// Call historic data service
//		HttpClient httpClient = HttpClientBuilder.create().build();
//		URIBuilder builder = new URIBuilder(dbManager.getUrl(platform));
				
		// A simple request
		logger.info("Retrieving historic data from platform: " + platform);
//		builder.setParameter("platform", platform).setParameter("deviceId", device);
		// Get conditions
		String fromDate = "2017-01-01"; // Default value. TODO: remove unused query parameters (Dates should be optional parameters)
		String toDate = dateFormat.format(new Date()).toString(); // Default value
		for(String condition : params){
			String[] x = condition.split(" ");
			if(x[0].equalsIgnoreCase("date")){
				// Get start and end dates
				if(x[1].contains(">")){
					fromDate = x[2];
				}else if(x[1].contains("<")){
					toDate = x[2];
				}else if(x[1].contains("=")){
					fromDate = x[2];
					toDate = x[2];
				}
			}
//				else if(x[0].equalsIgnoreCase("ds")){
//				builder.setParameter("DS", x[2]);
//			}
		}
		
		response = historic.getFromPlatform(platform, device, fromDate, toDate);
		
//		builder.setParameter("from", fromDate);
//		builder.setParameter("to", toDate);
				
//		HttpGet httpGet = new HttpGet(builder.build());
//		HttpResponse httpResponse = httpClient.execute(httpGet);
//		int responseCode = httpResponse.getStatusLine().getStatusCode();
		
//		if(responseCode==200){
//			HttpEntity responseEntity = httpResponse.getEntity();
//			if(responseEntity!=null) {
//				response = EntityUtils.toString(responseEntity);
//			}
//		}else{
//			throw new Exception("Could not retrieve data. Response code from historic data service: " + responseCode);
//		}
						
		return response;
	}
	
	
	String getFromIndependentStorage(String db, String table, String query) throws Exception{
		// The db name in the Independent Data Storage is used as identifier in the DB registry
		String response = "";
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
		 // Do something with the response code?
		 int responseCode = httpResponse.getStatusLine().getStatusCode();
		 logger.info("Response code: " + httpResponse.getStatusLine().getStatusCode());
		 if(responseCode==200){
			 if(responseEntity!=null) {
				 response = EntityUtils.toString(responseEntity);
			 }
		 }else{
				throw new Exception("Could not retrieve data. Response code received from Independent Data Storage: " + responseCode);
		 }	
		return response;
	}

}

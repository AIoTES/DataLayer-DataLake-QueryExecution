package eu.activage.datalake;

import java.text.SimpleDateFormat;
import java.util.Date;

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

class DataLakeClient {
	
	String historicUrl;
	// TODO: integrate with independent data storage and indexing service
	String storageUrl;
	
	private final Logger logger = LoggerFactory.getLogger(DataLakeClient.class);
	
	DataLakeClient(String historicData, String independentData){
		historicUrl = historicData;
		storageUrl = independentData;
	}
	
	String execute(Query q) throws Exception{
		String response = null;
		
		// TODO: analyze query and identify data sources (by the index) Table = index
		// In the current version, indices represent platforms (for the historic data module) and columns represent devices (or magnitudes?)
		
		// A simple call to the historic data service
//		response = getHistoricData(q.index, q.columns[0], q.conditions);
		
		// TODO: integrate indexing service
		if(q.index.equals("fiware") || q.index.equals("sofia2") || q.index.equals("universaal")){
			// Generate queries for each column and compose response (for real historic data)
			JsonArray res = new JsonArray();
			JsonParser parser = new JsonParser();
			for (int i = 0; i < q.columns.length; i++){
				String data = getHistoricData(q.index, q.columns[i], q.conditions);
				res.addAll(parser.parse(data).getAsJsonArray());
			}
			response = res.toString();
		}else{
			// TODO: integrate independent data storage
			// TODO: map sql query to independent data storage call
			// What are the "database" and "table" values?
			response = getFromIndependentStorage(q.index, q.index, q.createQueryString()); // DB, table, query
		}
				
		return response;
	}	
	
	String getHistoricData(String platform, String device, String[] params) throws Exception{
		String response = "";
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); // Format of the input query date values. TODO: add time?
		// TODO: check date format of the historic data service
		
		// Call historic data service
		HttpClient httpClient = HttpClientBuilder.create().build();
		URIBuilder builder = new URIBuilder(historicUrl);
				
		// A simple request
		logger.info("Retrieving historic data from platform: " + platform);
		builder.setParameter("platform", platform).setParameter("deviceId", device);
		// Get conditions
		String fromDate = "2017-01-01"; // Default value
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
			}else if(x[0].equalsIgnoreCase("ds")){
				builder.setParameter("DS", x[2]);
			}
		}
		builder.setParameter("from", fromDate);
		builder.setParameter("to", toDate);
				
		HttpGet httpGet = new HttpGet(builder.build());
		HttpResponse httpResponse = httpClient.execute(httpGet);
		int responseCode = httpResponse.getStatusLine().getStatusCode();
		
		if(responseCode==200){
			HttpEntity responseEntity = httpResponse.getEntity();
			if(responseEntity!=null) {
				response = EntityUtils.toString(responseEntity);
			}
		}else{
			throw new Exception("Could not retrieve data. Response code from historic data service: " + responseCode);
		}
						
		return response;
	}
	
	
	String getFromIndependentStorage(String db, String table, String query) throws Exception{
		// TODO: test with independent database
		String response = "";
				
		JsonObject body = new JsonObject();
		body.addProperty("db", db);
		body.addProperty("table", table);
		body.addProperty("query", query);
		
		// test
//		response = body.toString();
		
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost httpPost = new HttpPost(storageUrl + "independentStorage/query");
		
		 HttpEntity translationEntity = new StringEntity(body.toString(), ContentType.APPLICATION_JSON);
		 httpPost.setEntity(translationEntity);
		 HttpResponse httpResponse = httpClient.execute(httpPost);
		 // Do something with the response code?
		 int responseCode = httpResponse.getStatusLine().getStatusCode();
		 logger.info("Response code: " + httpResponse.getStatusLine().getStatusCode());
		 if(responseCode==200){
			 HttpEntity responseEntity = httpResponse.getEntity();
			 if(responseEntity!=null) {
				 response = EntityUtils.toString(responseEntity);
			 }
		 }else{
				throw new Exception("Could not retrieve data. Response code received from Independent Data Storage: " + responseCode);
		 }	
		return response;
	}

}

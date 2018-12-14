package eu.activage.datalake;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DataLakeClient {
	
	String historicUrl = "http://localhost:4569/historic"; // TODO: get this value from configuration or as input parameter in main class
	
	private final Logger logger = LoggerFactory.getLogger(DataLakeClient.class);
	
	String execute(Query q) throws Exception{
		String response = null;
		
		// TODO: analyze query and identify data sources (by the index)
		// In the current version, indices represent platforms (for the historic data module) and columns represent devices (or magnitudes?)
		
		// A simple call to the historic data service
		response = getHistoricData(q.index, q.columns[0], q.conditions);
				
		return response;
	}	
	
	String getHistoricData(String platform, String device, String[] params) throws URISyntaxException, ParseException, ClientProtocolException, IOException{
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
		 
		// Do something with the response code?
		HttpEntity responseEntity = httpResponse.getEntity();
		if(responseEntity!=null) {
			response = EntityUtils.toString(responseEntity);
		}
		
		return response;
	}

}

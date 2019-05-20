package eu.activage.datalake.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import eu.activage.datalake.historicdata.DatabaseManager;
import eu.activage.datalake.historicdata.HistoricData;

class DataLakeClient {
	
	// TODO: integrate with indexing service
	DatabaseManager dbManager;
	HistoricData historic;
	
	private final Logger logger = LoggerFactory.getLogger(DataLakeClient.class);
	
	DataLakeClient(){
		// Use mock service registry
		dbManager = new DatabaseManager();
		historic = new HistoricData();
	}
	
	DataLakeClient(String url){
		// Use service registry prototype
		dbManager = new DatabaseManager(url);
		historic = new HistoricData(url);
	}
	
	// Translate query into calls to webservices
	JsonArray translate(Query q) throws Exception{
		// Analyze query and identify data sources
		// It's possible that a single query translates to multiple calls
		JsonArray apiCalls = new JsonArray();
		String[] dbIds = dbManager.getDBIds(q.getDs(),q.getplatforms());
		
		logger.info("Query parameterms: " + q.toString()); //DEBUG
		
		String[] columns = q.getDeviceTypes();
		if(columns== null) columns = q.getDeviceIds();
		
		for(String dbId:dbIds){
			// Get database type and location
			// TODO: integrate indexing service
						
			for (int i = 0; i < columns.length; i++){
				String fromDate = q.getStartDate();
				String toDate = q.getEndDate();
				JsonObject result;
				// USE ONLY DEVICE TYPE OR DEVICE ID. TODO: CHECK IF WE CAN USE BOTH PARAMETERS IN THE SAME QUERY AND HOW
				if (q.getDeviceTypes() == null) result = historic.getWebserviceCall(dbId, columns[i], null, fromDate, toDate);
				else result = historic.getWebserviceCall(dbId, null, columns[i], fromDate, toDate);
				apiCalls.add(result);
			}
			
		}
		
		return apiCalls;
	}
	
	
	String execute(Query q) throws Exception{
		String response = null;
		String[] dbIds = dbManager.getDBIds(q.getDs(),q.getplatforms()); // Get service identifiers by DS and platform type
		
		// Get database type and location
		// It's possible that a single query translates to multiple calls
		logger.info("Query parameterms: " + q.toString()); //DEBUG
		JsonArray res = new JsonArray();
		
		String[] columns = q.getDeviceTypes();
		if(columns == null) columns = q.getDeviceIds();
		
		for(String dbId:dbIds){
			// TODO: integrate indexing service
			for (int i = 0; i < columns.length; i++){
				JsonArray data;
				// USE ONLY DEVICE TYPE OR DEVICE ID. TODO: CHECK IF WE CAN USE BOTH PARAMETERS IN THE SAME QUERY AND HOW
				if (q.getDeviceTypes() != null) data = historic.getData(dbId, null, columns[i], q.getStartDate(), q.getEndDate());
				else data = historic.getData(dbId, columns[i], null, q.getStartDate(), q.getEndDate());
				res.addAll(data);
			}
		}
		response = res.toString();				
		return response;
	}	
	
	
}

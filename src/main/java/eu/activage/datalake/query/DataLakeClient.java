package eu.activage.datalake.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;

import eu.activage.datalake.historicdata.DatabaseManager;
import eu.activage.datalake.historicdata.HistoricData;

class DataLakeClient {
	
	// TODO: integrate with indexing service
	DatabaseManager dbManager;
	HistoricData historic = new HistoricData();
	
	private final Logger logger = LoggerFactory.getLogger(DataLakeClient.class);
	
	DataLakeClient(){
		dbManager = new DatabaseManager();
	}
	
	// Translate query into calls to webservices
	JsonArray translate(Query q) throws Exception{
		JsonArray apiCalls = new JsonArray();
		String[] dbIds = q.getTableIds();
		
		logger.info("Query parameterms: " + q.toString()); //DEBUG
		
		String[] columns = q.getDeviceTypes();
		if(columns== null) columns = q.getDeviceIds();
		
		for(String dbId:dbIds){
			// It's possible that a single query translates to multiple calls
			// Get database type and location
			
			// TODO: integrate indexing service
			
			// Analyze query and identify data sources (by the index)
			// Using platform or DS as index
			
			for (int i = 0; i < columns.length; i++){
				String fromDate = q.getStartDate();
				String toDate = q.getEndDate();
				String result;
				// USE ONLY DEVICE TYPE OR DEVICE ID. TODO: CHECK IF WE CAN USE BOTH PARAMETERS IN THE SAME QUERY AND HOW
				if (q.getDeviceTypes() == null) result = historic.getURL(dbId, columns[i], null, fromDate, toDate).toString();
				else result = historic.getURL(dbId, null, columns[i], fromDate, toDate).toString();
				apiCalls.add(result);
			}
			
		}
		
		return apiCalls;
	}
	
	
	String execute(Query q) throws Exception{
		String response = null;
		// DS or platform (name) = index
		// In the current version, indices represent platforms (for the historic data module) and columns represent devices (or magnitudes?)
		String[] dbIds = q.getTableIds();
		
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

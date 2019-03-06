package eu.activage.datalake.historicdata;

import java.io.IOException;
import java.net.URL;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DatabaseManager {
	
	JsonArray dbs;
	
	// TODO: USE A REAL DATABASE
	// TODO: ADD MANAGEMENT FUNCTIONS (REGISTER, UNREGISTER...)
	
	public DatabaseManager(){
		JsonParser parser = new JsonParser();
		// Get available databases
		URL url = Resources.getResource("databases.json");
	    try {
			String data = Resources.toString(url, Charsets.UTF_8);
			dbs = parser.parse(data).getAsJsonArray();
						
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public JsonObject getDb(String id){
		JsonObject target = null;
		
		for(int i=0; i<dbs.size(); i++){
			JsonObject db = dbs.get(i).getAsJsonObject();
			String elementId = db.get("id").getAsString();
			if(id.equals(elementId)) target = db;
		}
		return target;
	}
		
	public boolean isIndependentDataStorage(String id) throws Exception{
		JsonObject db = getDb(id);
		if(db != null){
			String type = db.get("type").getAsString();
			if(type.equals("common")) return true;
			else return false;
		} 
		else throw new Exception("Data origin not found");
	}
	
	public String getUrl(String id){ // Get db location
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get("url").getAsString();	
		return response;
	}
	
	public String getPlatformId(String id){ // To get data from the SIL registry
		String response = null;
		JsonObject db = getDb(id);
		if(db != null) response = db.get("platformId").getAsString();	
		return response;
	}
	
	public String getPlatformType(String id){ // For syntactic translation
		String type = null;
		JsonObject db = getDb(id);
		if(db != null) type = db.get("platformType").getAsString();	
		return type;
	}
	
	public String[] getUptreamAlignment(String id){ // For semantic translation
		String[] alignment = null;
		JsonObject db = getDb(id);
		if(db != null){
			alignment = new String[2];
			alignment[0] = db.get("upstreamAlignmentName").getAsString();
			alignment[1] = db.get("upstreamAlignmentVersion").getAsString();
		}	
		return alignment;
	}
	
	public String[] getDownstreamAlignment(String id){
		String[] alignment = null;
		JsonObject db = getDb(id);
		if(db != null){
			alignment = new String[2];
			alignment[0] = db.get("downstreamAlignmentName").getAsString();
			alignment[1] = db.get("downstreamAlignmentVersion").getAsString();
		}	
		return alignment;
	}
	
}

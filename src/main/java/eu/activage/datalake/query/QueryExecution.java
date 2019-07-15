package eu.activage.datalake.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import spark.Service;


public class QueryExecution {
	private int port;
    private Service spark;
    private final Logger logger = LoggerFactory.getLogger(QueryExecution.class);
    private DataLakeClient client;
    
    
    public QueryExecution(int port, String url) {
        this.port = port;
        if(url!=null){
        	if (!url.endsWith("/")) url = url + "/";
        	client = new DataLakeClient(url);
        } else client = new DataLakeClient();
    }
    
    public void start() throws Exception {
    	    	
    	spark = Service.ignite().port(port);
    	// Swagger UI
    	spark.staticFileLocation("/public");
    	spark.get("/swagger",(req, res) -> {res.redirect("index.html"); return null;});
    	
    	spark.post("/getSchema", (request, response) -> {
    		// TODO	
    		logger.info("Get Schema");
    		
    		// Return IDS DBs and tables
			String schema;
			try {
				JsonParser parser = new JsonParser();
				JsonObject reqBody = (JsonObject) parser.parse(request.body());
				String db = reqBody.get("db").getAsString();
				if(db != null && !db.isEmpty()){
					// Return tables in a DB
					schema = client.getSchema(db);
				}else{
					// Return all DBs and tables
					schema = client.getSchema();
				}
			} catch (Exception e) {
				response.status(400);
                e.printStackTrace();
                return e.getMessage();
			}
    		
    		response.header("Content-Type", "application/json;charset=UTF-8");
    		response.status(200);
    		return schema;
    	});
    	    	    	
    	spark.post("/query", (request, response) -> {
    		JsonObject responseBody = new JsonObject();    			   					    			
    		try {
    			// Extract query
    			JsonParser parser = new JsonParser();
    			JsonObject reqBody = (JsonObject) parser.parse(request.body());
    			// Parse query
    			Query query = new Gson().fromJson(reqBody, Query.class);			    				
    			// Execute query   		
    			String result = client.execute(query);
    			// Format response
    			JsonElement dbResponse = parser.parse(result).getAsJsonArray();
    			responseBody.add("records", dbResponse);	
    		} catch (Exception e) {
                response.status(400);
                e.printStackTrace();
                return e.getMessage();
            }
    		
   		 	response.header("Content-Type", "application/json;charset=UTF-8");
   		 	response.status(200);
   		 	return responseBody.toString();
    	});
    		
    	spark.post("/querytranslation", (request, response) -> { 
    		JsonArray result;		    			
    		try {
    			// Extract query
    			JsonParser parser = new JsonParser();
    			JsonObject reqBody = (JsonObject) parser.parse(request.body());
    			// Parse query
    			Query query = new Gson().fromJson(reqBody, Query.class);			    				
    			// Translate query   		
    			result = client.translate(query);	
    		} catch (Exception e) {
                response.status(400);
                e.printStackTrace();
                return e.getMessage();
            }
   		 	response.header("Content-Type", "application/json;charset=UTF-8");
   		 	response.status(200);
   		 	return result.toString();
    	});
    	
    	//HTTP OPTIONS added for integration with nodejs applications
    	spark.options("/getSchema", (request, response) -> {   
    		response.header("Access-Control-Allow-Origin", "*");
    		response.header("Access-Control-Allow-Methods", "POST");
    		response.header("Access-Control-Allow-Headers", "Origin, Content-Type, Authorization, Accept");
    		response.header("Allow", "POST, OPTIONS");
//    		response.header("Content-Type", "application/json;charset=UTF-8");
    		response.status(200);
    		return "";
        });
    	
    	spark.options("/query", (request, response) -> {   
    		response.header("Access-Control-Allow-Origin", "*");
    		response.header("Access-Control-Allow-Methods", "POST");
    		response.header("Access-Control-Allow-Headers", "Origin, Content-Type, Authorization, Accept");
    		response.header("Allow", "POST, OPTIONS");
//    		response.header("Content-Type", "application/json;charset=UTF-8");
    		response.status(200);
    		return "";
        });
    	
    	spark.options("/querytranslation", (request, response) -> {   
    		response.header("Access-Control-Allow-Origin", "*");
    		response.header("Access-Control-Allow-Methods", "POST");
    		response.header("Access-Control-Allow-Headers", "Origin, Content-Type, Authorization, Accept");
    		response.header("Allow", "POST, OPTIONS");
//    		response.header("Content-Type", "application/json;charset=UTF-8");
    		response.status(200);
    		return "";
        });
    	
    }
    
    public void stop() {
        spark.stop();
    }

    public static void main(String[] args) throws Exception {
    	int port = 4570;
    	String url = null;
    	if (args.length > 0){
    		url = args[0];
    		if (args.length > 1){
    			port = Integer.parseInt(args[1]);
    		}
    	}
    	new QueryExecution(port, url).start();
    }
    
    
}

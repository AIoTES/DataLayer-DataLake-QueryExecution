package eu.activage.datalake.query;

import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import spark.Service;


public class QueryExecution {
	private int port;
//	private String serviceRegistryUrl;
    private Service spark;
    private final Logger logger = LoggerFactory.getLogger(QueryExecution.class);
    private DataLakeClient client;
    
    
    public QueryExecution(int port, String url) {
        this.port = port;
        if(url!=null){
        	if (!url.endsWith("/")) url = url + "/";
//        	this.serviceRegistryUrl = url;
        	client = new DataLakeClient(url);
        } else client = new DataLakeClient();
    }
    
    public void start() throws Exception {
    	    	
    	spark = Service.ignite().port(port);
    	// Swagger UI
    	spark.staticFileLocation("/public");
    	
    	spark.get("/swagger",(req, res) -> {res.redirect("index.html"); return null;});
    	
 //   	spark.path("/api", () -> { //  /dataIntegrationEngine
    	
    		spark.post("/getSchema", (request, response) -> {
    			// TODO
    			
    			logger.info("Get Schema");
    			
    			// TODO: call some (future) service to get the requested schema
    			// Get test data from a file
    			URL test = Resources.getResource("test-schema.json");
    		    String schema = Resources.toString(test, Charsets.UTF_8);
    			
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
    				
//    				String sql = reqBody.get("query").getAsString();
//    				logger.info("Received query: " + sql);    			
//    				// Parse query
//    				Query query = new Query(sql);
    				    				
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
//    			JsonObject responseBody = new JsonObject();  
    			JsonArray result;
    			    			
    			try {
    				// Extract query
    				JsonParser parser = new JsonParser();
    				JsonObject reqBody = (JsonObject) parser.parse(request.body());
    				// Parse query
    				Query query = new Gson().fromJson(reqBody, Query.class);
    				
//    				String sql = reqBody.get("query").getAsString();
//    				logger.info("Received query: " + sql);
//    				// Parse query
//    				Query query = new Query(sql);
    				    				
    				// Execute query   		
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
    	
 //   	});
    
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

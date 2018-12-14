package eu.activage.datalake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import spark.Service;


public class QueryExecution {
	private int port;
    private Service spark;
    private final Logger logger = LoggerFactory.getLogger(QueryExecution.class);
    private DataLakeClient client;
    
    
    public QueryExecution(int port) {
        this.port = port;
        client = new DataLakeClient();
    }
    
    public void start() throws Exception {
    	
    	spark = Service.ignite().port(port);
    	
 //   	spark.path("/api", () -> { //  /dataIntegrationEngine
    	
    		spark.post("/getSchema", (request, response) -> {
    			// TODO
    			
    			logger.info("Get Schema");
    			
    			response.header("Content-Type", "application/json;charset=UTF-8");
    			response.status(200);
    			return "{ \"schema\": {} }";
    		});
    	
    	
    		spark.post("/query", (request, response) -> {
    			JsonObject responseBody = new JsonObject();    			   			
    			    			
    			try {
    				// Extract query
    				JsonParser parser = new JsonParser();
    				JsonObject reqBody = (JsonObject) parser.parse(request.body());
    				String sql = reqBody.get("query").getAsString();
    			
    				logger.info("Received query: " + sql);
    			
    				// Parse query
    				Query query = new Query(sql);
    				    				
    				// Execute query   		
    				String result = client.execute(query);
    		
    				// Format response
    				JsonElement dbResponse = parser.parse(result);
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
    	
 //   	});
    
    }
    
    public void stop() {
        spark.stop();
    }

    public static void main(String[] args) throws Exception {
    	int port = 4570;
    	if (args.length > 0){
    		port = Integer.parseInt(args[0]);
    	}
    	new QueryExecution(port).start();
    }
    
    

}

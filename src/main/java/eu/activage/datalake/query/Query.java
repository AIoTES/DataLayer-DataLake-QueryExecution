package eu.activage.datalake.query;

import com.google.gson.Gson;

class Query {
	
	String[] deviceID;
	String[] deviceType;
	String startDate;
	String endDate;
	String[] platform;
	String[] ds;
	
    
    // Utility methods
 	public String getStartDate() throws Exception{		
 		// TODO: check date format of the historic data service
 		return startDate;
 	}
 	
 	public String getEndDate() throws Exception{		
 		// TODO: check date format of the historic data service
 		return endDate;
 	}
 	
    public String[] getplatforms(){
    	return platform;
    }
    
    public String[] getDs(){
    	return ds;
    }
 	
    public String[] getDeviceIds(){
    	return deviceID;
    }
    
    public String[] getDeviceTypes(){
    	return deviceType;
    }
 	
    // Test
    public String toString(){   	    	
    	Gson gson = new Gson();	
    	return gson.toJson(this);
    }
    
}

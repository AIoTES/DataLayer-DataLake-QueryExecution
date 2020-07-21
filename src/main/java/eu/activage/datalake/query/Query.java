/*
 * Copyright 2020 Universitat Politècnica de València
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

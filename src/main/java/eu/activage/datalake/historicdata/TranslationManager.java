package eu.activage.datalake.historicdata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.interiot.message.Message;
import eu.interiot.message.MessageMetadata;
import eu.interiot.message.MessagePayload;
import eu.interiot.message.ID.EntityID;
import eu.interiot.message.managers.URI.URIManagerMessageMetadata;
import eu.interiot.message.metadata.PlatformMessageMetadata;
import eu.interiot.services.syntax.FIWAREv2Translator;
import eu.interiot.services.syntax.Sofia2Translator;

public class TranslationManager {
	
	/*
	 * This class emulates the translation services of the (future) SIL v2
	 * 
	 * Syntactic translation available for universAAL, Fiware and SOFIA2
	 * 
	 * Input needed for syntactic translation:
	 * - Data from the platform (same format as real-time measurements for historic data)
	 * - Platform type identifier (same values as in the bridges)
	 * 
	 * Semantic translation using IPSM and semantic alignments (semantic alignment name and version provided by the historic data service emulator)
	 * 
	 * */
	
	private final Logger logger = LoggerFactory.getLogger(TranslationManager.class);
	
	private String ipsmUrl; // http://localhost:8888/
	private String platformId;
	String serviceRegistryUrl;
	final String DB = "services";
	final String IPSM_TYPE = "semantic-translator";
	
	public TranslationManager(String url){
		// Get IPSM URL from registry
		// TODO: get syntactic translation web services URLs from the registry
		serviceRegistryUrl = url + DB;
		ipsmUrl = null;
		platformId = null;
	}
	
	public TranslationManager(){
		Properties prop = new Properties();
		InputStream input = null;
		serviceRegistryUrl = null;
				
		try {
			input = getClass().getClassLoader().getResourceAsStream("config.properties");
			// load a properties file
			prop.load(input);
			ipsmUrl = prop.getProperty("ipsm-url");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
    		if (input != null) {
    			try {
    				input.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
		platformId = "";
	}
	
	void setPlatformId(String id){
		platformId = id;
	}
	
	public String syntacticTranslation(String data, String type) throws Exception{
		
		String response;
		// SELECT SYNTACTIC TRANSLATOR USING THE PLATFORM TYPE IDENTIFIER (SAME VALUE AS IN THE BRIDGE)
		switch(type){
       	case " http://inter-iot.eu/FIWARE":
       		response = translateFromFiware(data);
       		break;
       	case "http://inter-iot.eu/sofia2":
       		response = translateFromSofia(data);
       		break;
       	case "http://inter-iot.eu/UniversAAL":
       		response = translateFromUniversaal(data);
       		break;
       		// etc
       	default:
       		throw new Exception("Platform type not supported: " + type);	
       } 
		
		return response;
	}
	
	private String translateFromFiware(String data) throws Exception{
		// Translate data to JSON-LD
//        logger.debug("Translate data from Fiware...  ");
        FIWAREv2Translator translator2 = new FIWAREv2Translator();
        Model transformedModel = translator2.toJenaModelTransformed(data);
        
        // TODO: Change identifier (hasId). Use the same format as the messages from the bridge

        // Create Inter-IoT message
	    return createObservationMessage(transformedModel);
	}
	
	private String translateFromSofia(String data) throws Exception{
		// Translate data to JSON-LD
//        logger.debug("Translate data from SOFIA2...  ");
        Sofia2Translator translator = new Sofia2Translator();
        Model transformedModel = translator.toJenaModelTransformed(data);

        // Create Inter-IoT message
	    return createObservationMessage(transformedModel);
	}
	
	private String translateFromUniversaal(String data) throws IOException{
		// Transform data to JSON-LD
//        logger.debug("Translate data from universAAL...  ");
	    Model eventModel = ModelFactory.createDefaultModel();
	    eventModel.read(new ByteArrayInputStream(data.getBytes()), null, "TURTLE");
	     
	     // Create Inter-IoT message
	     return createObservationMessage(eventModel);
	}
	
	private String createObservationMessage(Model model) throws IOException{
	    	// ADD METADATA GRAPH
	    	Message callbackMessage = new Message();
	    	// Metadata
	        PlatformMessageMetadata metadata = new MessageMetadata().asPlatformMessageMetadata();
	        metadata.initializeMetadata();
	        metadata.addMessageType(URIManagerMessageMetadata.MessageTypesEnum.OBSERVATION);
	        if(!platformId.isEmpty()) metadata.setSenderPlatformId(new EntityID(platformId)); // Add senderPlatformId
	        callbackMessage.setMetadata(metadata);
	        
	        //Finish creating the message
	        MessagePayload messagePayload = new MessagePayload(model);
	        callbackMessage.setPayload(messagePayload);  
	        
	        ObjectMapper mapper = new ObjectMapper();
	        ObjectNode jsonMessage = (ObjectNode) mapper.readTree(callbackMessage.serializeToJSONLD());
	        return jsonMessage.toString();
	    }
	
	public String semanticTranslation(String data, String alignName, String alignVersion) throws Exception{
		   String result = data;
		   HttpClient httpClient = HttpClientBuilder.create().build();
		   String url = getIpsmUrl();
		   if(url!=null && !url.equals("")){
			   // Call IPSM for semantic translation
//			   logger.info("Sending data to IPSM...");
			   HttpPost ipsmPost = new HttpPost(url + "translation");
			   JsonObject translationData = new JsonObject();
			   JsonObject alignId = new JsonObject(); 
			   alignId.addProperty("name", alignName);
			   alignId.addProperty("version", alignVersion);
			   JsonArray array = new JsonArray();
			   array.add(alignId);
			   translationData.add("alignIDs", array);
			   translationData.addProperty("graphStr", result);
			   HttpEntity translationEntity = new StringEntity(translationData.toString(), ContentType.APPLICATION_JSON);
			   ipsmPost.setEntity(translationEntity);
			   HttpResponse httpResponse = httpClient.execute(ipsmPost);
			   int responseCode = httpResponse.getStatusLine().getStatusCode();
//			   logger.info("Response code: " + httpResponse.getStatusLine().getStatusCode());
			   if(responseCode==200){
				   HttpEntity responseEntity = httpResponse.getEntity();
				   if(responseEntity!=null) {
					   JsonParser parser = new JsonParser();
					   JsonObject responseBody = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonObject();
//					   logger.info("Message: " + responseBody.get("message").getAsString());
					   result = responseBody.get("graphStr").getAsString();
				   }
			   }else{
					throw new Exception("Response code received from IPSM: " + responseCode);
				}
		   }else{
			   logger.warn("Could not send data to IPSM: no URL. No semantic translation was performed.");
		   }
		   return result;
	   }
	
	private String getIpsmUrl() throws Exception{
		String url = null;
		
		if(serviceRegistryUrl!=null){
			// Use JSON server
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet(serviceRegistryUrl + "?type=" + IPSM_TYPE);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			int responseCode = httpResponse.getStatusLine().getStatusCode();
			   if(responseCode==200){
				   HttpEntity responseEntity = httpResponse.getEntity();
				   if(responseEntity!=null) {
					   JsonParser parser = new JsonParser();
					   JsonObject target = parser.parse(EntityUtils.toString(responseEntity)).getAsJsonArray().get(0).getAsJsonObject();
					   url = target.get("url").getAsString();
				   }else {
					   logger.warn("No semantic translation service found.");
					   url = null;
				   }
			   }else{
				   throw new Exception("Response code received from Registry: " + responseCode);
			   }
		}else{
			url = ipsmUrl;
		}
		
		return url;
	}
	
}

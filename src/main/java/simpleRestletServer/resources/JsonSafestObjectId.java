package simpleRestletServer.resources;

import java.util.LinkedHashMap;

import org.bson.types.ObjectId;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import nerAndGeo.NERThreadList;

public class JsonSafestObjectId extends ServerResource{
	@Get ("application/json")
    public String represent(){
		ObjectId safestObjectId = NERThreadList.getSafeObjectId(NERThreadList.list);
			
		LinkedHashMap<String, ObjectId> outJson = new LinkedHashMap<String, ObjectId>(); 
		outJson.put("safestObjectId", safestObjectId);
		
	    return outJson.toString();
    }
}

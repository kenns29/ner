package simpleRestletServer.resources;

import java.io.IOException;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONValue;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import nerAndGeo.NERThreadList;

public class JsonStatus extends ServerResource{
	private static Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	@Get ("application/json")
    public String represent(){
		String response = "";
        StringWriter out = new StringWriter();
        LinkedList<LinkedHashMap<String, Object>> statusList = NERThreadList.nerThreadStatusToLinkedList();
        try {
			JSONValue.writeJSONString(statusList, out);
		} catch (IOException e) {
			LOGGER.error("did not load json status successfully.", e);
		}
        String jsonString = out.toString();
        response += jsonString;
		return response;
	}
}

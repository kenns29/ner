package simpleHttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import nerAndGeo.NERThreadList;

import org.json.simple.JSONValue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class HttpServerHandlerJSON implements HttpHandler {
	@Override
	public void handle(HttpExchange httpExchange) throws IOException {
		String response = "";
		httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        
        StringWriter out = new StringWriter();
        LinkedList<LinkedHashMap<String, Object>> statusList = NERThreadList.nerThreadStatusToLinkedList();
        JSONValue.writeJSONString(statusList, out);
        String jsonString = out.toString();
        response += jsonString;
        os.write(response.getBytes());
        os.flush();
        os.close();
	}
	
}

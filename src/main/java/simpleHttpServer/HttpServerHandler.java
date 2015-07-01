package simpleHttpServer;

import java.io.IOException;
import java.io.OutputStream;

import util.ThreadStatus;
import nerAndGeo.NER;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class HttpServerHandler implements HttpHandler {

	@Override
	public void handle(HttpExchange httpExchange) throws IOException {
		//String response = "<html><body><table><tr><td>1</td><td>2</td></tr><tr><td>3</td><td>4</td></tr></table></body></html>";
		String response = "<html><body>";
		String threadTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
		threadTable += ThreadStatus.makeHttpTableHeader();
		for(int i = 0; i < NER.NERThreadList.size(); i++){
			threadTable += NER.NERThreadList.get(i).threadStatus.toHttpTableRowEntry();
		}
		threadTable += "</table>";
		response += threadTable;
		response += "</body></html>";
        httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
		
	}

}

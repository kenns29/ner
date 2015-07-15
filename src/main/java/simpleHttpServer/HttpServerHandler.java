package simpleHttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import org.bson.types.ObjectId;

import util.ThreadStatus;
import util.TimeRange;
import nerAndGeo.Geoname;
import nerAndGeo.Main;
import nerAndGeo.NER;
import nerAndGeo.NERThreadList;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class HttpServerHandler implements HttpHandler {

	@Override
	public void handle(HttpExchange httpExchange) throws IOException {
		String response = "";
		synchronized(NER.class){
			String threadTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
			threadTable += ThreadStatus.makeHttpTableHeader();
			for(int i = 0; i < NERThreadList.list.size(); i++){
				threadTable += NERThreadList.list.get(i).threadStatus.toHttpTableRowEntry();
			}
			threadTable += "</table>";
			
			String queueTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
			queueTable += TimeRange.makeHttpTableHeader();
			Iterator<TimeRange> queueIterator = Main.queue.iterator();
			while(queueIterator.hasNext()){
				TimeRange timeRange = queueIterator.next();
				queueTable += timeRange.toHttpTableRowEntry();
			}
			queueTable += "</table>";
			
			ObjectId safeObjectId = NERThreadList.getSafeObjectId(NERThreadList.list);
			
			response = "<html><body>";
			response += "<p> Current Thread Status </p>";
			response += threadTable;
			response += "<p> The Current Safest Object Id is " + safeObjectId.toString() + "</p>";
			response += "<p> Current Tasks Queued </p>";
			response += queueTable;
			response += "<p> Current Geoname Account used for the service is " + Geoname.accountName + "</p>";
			
			if(Main.geonameServiceCheckerThread.isAlive()){
				String geonameServiceChecker = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
				geonameServiceChecker += "<tr>"
									  +	 "<td>System Thread ID</td>"
									  +  "<td>Status</td>"
									  +  "</tr><tr>"
									  +  "<td>" + Main.geonameServiceCheckerThread.getId() + "</td>"
									  +  "<td>Alive</td>"
									  +  "</tr>";
				response += "<p> Geoname Service Check Running </p>";
				response += geonameServiceChecker;
			}
			
			response += "</body></html>";
			
			
		}
		httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
		
	}

}

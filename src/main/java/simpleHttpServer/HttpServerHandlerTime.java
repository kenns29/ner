package simpleHttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import nerAndGeo.Main;

public class HttpServerHandlerTime implements HttpHandler {
	@Override
	public void handle(HttpExchange httpExchange) throws IOException {
		String response = " ";
		//synchronized(Main.lockObjectDocumentProcessTime){
			DecimalFormat df = new DecimalFormat("#.00");
			String overallTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
			overallTable += "<tr>"
						  + "<td>Average Time For A Thread to Finish</td>"
						  + "<td>" + df.format(Main.totalThreadFinishedTime / Main.threadFinishCount.intValue()) + "milliseconds</td>"
			              + "</tr><tr>"
						  + "<td>Average Time for Task Manager to Insert a Task</td>"
			              + "<td>" + df.format(Main.totalTaskManagerFinishedTime / Main.taskMangerFinishCount.intValue()) + " milliseconds</td>"
			              + "</tr>";
			overallTable += "</table>";
			
			String documentTimeTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
			//add document Table header
			documentTimeTable += "<tr>"
					           + "<td>Type</td>"
					           + "<td>Total Time</td>"
					           + "<td>Percentage over Total Document Process Time</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>Process Document</td>"
					           + "<td>" + Main.totalDocumentProcessTime + "</td>"
					           + "<td>" + df.format((double)Main.totalDocumentProcessTime / Main.totalDocumentProcessTime * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>Update Mongo</td>"
					           + "<td>" + Main.totalMongoUpdateTime + "</td>"
					           + "<td>" + df.format((double)Main.totalMongoUpdateTime / Main.totalDocumentProcessTime * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>NER</td>"
					           + "<td>" + Main.totalNerTime + "</td>"
					           + "<td>" + df.format((double)Main.totalNerTime / Main.totalDocumentProcessTime * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>User NER</td>"
					           + "<td>" + Main.totalUserNerTime + "</td>"
					           + "<td>" + df.format((double)Main.totalUserNerTime / Main.totalDocumentProcessTime * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>Geoname</td>"
					           + "<td>" + Main.totalNerGeonameTime + "</td>"
					           + "<td>" + df.format((double)Main.totalNerGeonameTime / Main.totalDocumentProcessTime * 100) + "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>Geojson</td>"
					           + "<td>" + Main.totalGeojsonTime + "</td>"
					           + "<td>" + df.format((double)Main.totalGeojsonTime / Main.totalDocumentProcessTime * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "</table>";
			
			response += "<html><body>";
			response += "<p>Overall Time</p>";
			response += overallTable;
			response += "<p>Overall Document Process Time</p>";
			response += documentTimeTable;
			response += "</body></html>";
		//}
		httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
	}
}

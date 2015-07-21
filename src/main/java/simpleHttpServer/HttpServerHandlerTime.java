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
		//synchronized(Main.documentProcessTimeHandler.lockObjectDocumentProcessTime){
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
					           + "<td>" + Main.documentProcessTimeHandler.getTotalDocumentProcessTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getTotalDocumentProcessTime() / Main.documentProcessTimeHandler.getTotalDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>Update Mongo</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getTotalMongoUpdateTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getTotalMongoUpdateTime() / Main.documentProcessTimeHandler.getTotalDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>NER</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getTotalNerTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getTotalNerTime() / Main.documentProcessTimeHandler.getTotalDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>User NER</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getTotalUserNerTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getTotalUserNerTime() / Main.documentProcessTimeHandler.getTotalDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>Geoname List</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getTotalNerGeonameTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getTotalNerGeonameTime() / Main.documentProcessTimeHandler.getTotalDocumentProcessTime() * 100) + "%</td>"
					           + "</tr>";
			
			documentTimeTable += "<tr>"
					           + "<td>Geojson</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getTotalGeojsonTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getTotalGeojsonTime() / Main.documentProcessTimeHandler.getTotalDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			documentTimeTable += "</table>";
					
			String geonameTimeTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
			geonameTimeTable += "<tr>"
					          + "<td>Type</td>"
					          + "<td>Total Time</td>"
					          + "<td>Percentage over Total Geoname Time</td>"
					          + "</tr>";
			
			geonameTimeTable += "<tr>"
					          + "<td>Total Geoname Time</td>"
					          + "<td>" + Main.documentProcessTimeHandler.getTotalGeonameTime() + "</td>"
					          + "<td>" + df.format((double)Main.documentProcessTimeHandler.getTotalGeonameTime() / Main.documentProcessTimeHandler.getTotalGeonameTime() * 100) + "%</td>"
					          + "</tr>";
			
			geonameTimeTable += "<tr>"
					          + "<td>Geoname Cache Get Time</td>"
					          + "<td>" + Main.documentProcessTimeHandler.getGeonameCacheGetTime() + "</td>"
					          + "<td>" + df.format((double)Main.documentProcessTimeHandler.getGeonameCacheGetTime() / Main.documentProcessTimeHandler.getTotalGeonameTime() * 100) + "%</td>"
					          + "</tr>";
			
			geonameTimeTable += "<tr>"
					          + "<td>Geoname Cache Put Time</td>"
					          + "<td>" + Main.documentProcessTimeHandler.getGeonameCachePutTime() + "</td>"
					          + "<td>" + df.format((double)Main.documentProcessTimeHandler.getGeonameCachePutTime() / Main.documentProcessTimeHandler.getTotalGeonameTime() * 100) + "%</td>"
					          + "</tr>";
			
			geonameTimeTable += "<tr>"
					          + "<td>Null Cache Check Time</td>"
					          + "<td>" + Main.documentProcessTimeHandler.getNullCacheCheckTime() + "</td>"
					          + "<td>" + df.format((double)Main.documentProcessTimeHandler.getNullCacheCheckTime() / Main.documentProcessTimeHandler.getTotalGeonameTime() * 100) + "%</td>"
					          + "</tr>";
			
			geonameTimeTable += "<tr>"
					          + "<td>null Cache Put Time</td>"
					          + "<td>" + Main.documentProcessTimeHandler.getNullCachePutTime() + "</td>"
					          + "<td>" + df.format((double)Main.documentProcessTimeHandler.getNullCachePutTime() / Main.documentProcessTimeHandler.getTotalGeonameTime() * 100) + "%</td>"
					          + "</tr>";
			
			geonameTimeTable += "<tr>"
					          + "<td>Geoname Process Time</td>"
					          + "<td>" + Main.documentProcessTimeHandler.getGeonameTime() + "</td>"
					          + "<td>" + df.format((double)Main.documentProcessTimeHandler.getGeonameTime() / Main.documentProcessTimeHandler.getTotalGeonameTime() * 100) + "%</td>"
					          + "</tr>";
			
			geonameTimeTable += "</table>";
					          
			response += "<html><body>";
			response += "<p>Overall Time</p>";
			response += overallTable;
			response += "<p>Overall Document Process Time</p>";
			response += documentTimeTable;
			response += "<p>Geoname Time Break Down</p>";
			response += geonameTimeTable;
			response += "</body></html>";
		//}
		httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.flush();
        os.close();
	}
}

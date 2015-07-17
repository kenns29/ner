package simpleHttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;

import util.CollUtilities;
import nerAndGeo.Main;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class HttpServerHandlerProgress implements HttpHandler {
	@Override
	public void handle(HttpExchange httpExchange) throws IOException {
		String response = "";
		long totalDocumentCount = -1;
		if(Main.configPropertyValues.stopAtEnd){
			totalDocumentCount = Main.totalDocuments;
		}
		else{
			totalDocumentCount = CollUtilities.getTotalDocumentCount(Main.mainColl);
		}
		int mainDocumentCount = Main.documentCount.intValue();
		int mainLastTimelyDocCount = Main.lastTimelyDocCount.intValue();
		long elapsedTimeInMillis = System.currentTimeMillis() - Main.mainStartTime;
		double elapsedTimeInMinutes = ((double)elapsedTimeInMillis) / 60000;
		int avgDocsPerMinutes = (int) (mainDocumentCount / elapsedTimeInMinutes);
		double percentComplete = (double)(mainDocumentCount) / totalDocumentCount;
		int estimateTimeInMinutes = (int) ((totalDocumentCount - mainDocumentCount) / avgDocsPerMinutes);
		
		DecimalFormat df = new DecimalFormat("#.00");
		
		//Building Table
		String progressTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
		progressTable += "<tr>"
				+ "<td>Number of total documents processed</td>"
				+ "<td>" + mainDocumentCount + "</td>"
				+ "</tr>";
		
		progressTable += "<tr>"
				+ "<td>Number of documents processed in the last minute</td>"
				+ "<td>" + mainLastTimelyDocCount + "</td>"
				+ "</tr>";
		
		progressTable += "<tr>"
				+ "<td>Average documents per minute</td>"
				+ "<td>" + avgDocsPerMinutes + "</td>"
				+ "</tr>";
		
		progressTable += "<tr>"
				+ "<td>Percentable Complete</td>"
				+ "<td>" + df.format(percentComplete * 100) + "%</td>"
				+ "</tr>";
		
		progressTable += "<tr>"
				+ "<td>Estimate time to complete (minutes)</td>"
				+ "<td>" + estimateTimeInMinutes + "</td>"
				+ "</tr>";
		
		progressTable += "<tr>"
				+ "<td>Total Elapsed Time (minutes)</td>"
				+ "<td>" + df.format(elapsedTimeInMinutes) + "</td>"
				+ "</tr>";
		
		progressTable += "</table>";
		
		//Building Page
		response += "<html><body>" + progressTable + "</table></html>";
		httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.flush();
        os.close();
		
	}
}

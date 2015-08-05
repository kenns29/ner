package simpleRestletServer.resources;

import java.text.DecimalFormat;

import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import nerAndGeo.Main;
import util.CollUtilities;

public class Progress extends ServerResource{   
    @Get ("html")
    public String represent(){
	    return this.getResonseString();
    }
    private String getResonseString(){
    	String response = "";
		long totalDocumentCount = 0;
		if(Main.configPropertyValues.stopAtEnd){
			if(Main.totalDocuments > 0){
				totalDocumentCount = Main.totalDocuments;
			}
		}
		else{
			totalDocumentCount = CollUtilities.getTotalDocumentCount(Main.mainColl);
		}
		int mainDocumentCount = Main.documentCount.intValue();
		int mainLastTimelyDocCount = Main.lastTimelyDocCount.intValue();
		long elapsedTimeInMillis = System.currentTimeMillis() - Main.mainStartTime;
		double elapsedTimeInMinutes = ((double)elapsedTimeInMillis) / 60000;
		int avgDocsPerMinutes = (elapsedTimeInMinutes != 0) ? (int) (mainDocumentCount / elapsedTimeInMinutes) : 0;
		double percentComplete = (totalDocumentCount != 0) ? (double)(mainDocumentCount) / totalDocumentCount : 0;
		
		int estimateTimeInMinutes = (avgDocsPerMinutes != 0) ? (int) ((totalDocumentCount - mainDocumentCount) / avgDocsPerMinutes) : 0;
		
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
				+ "<td>Percentage Complete</td>"
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
		response += "<html><body>" + progressTable + "</table></html>";
		return response;
    }
}

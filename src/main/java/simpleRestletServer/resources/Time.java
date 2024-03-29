package simpleRestletServer.resources;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import nerAndGeo.Main;

public class Time extends ServerResource{
	@Get ("html")
    public String represent(){
		String response = " ";
		synchronized(Main.documentProcessTimeHandler.lockObjectDocumentProcessTime){
			DecimalFormat df = new DecimalFormat("#.00");
			String overallTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
			overallTable += "<tr>"
						  + "<td>Average Time For A Thread to Finish</td>"
						  + "<td>" + ((Main.threadFinishCount.intValue() != 0)? df.format(Main.totalThreadFinishedTime / Main.threadFinishCount.intValue()) : "N/A ") + "milliseconds</td>"
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
			
			String periodicDocumentTimeTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";

			periodicDocumentTimeTable += "<tr>"
					           + "<td>Type</td>"
					           + "<td>Periodic Total Time</td>"
					           + "<td>Percentage over Total Peiodic Document Process Time</td>"
					           + "</tr>";

			periodicDocumentTimeTable += "<tr>"
					           + "<td>Process Document</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getPeriodicDocumentProcessTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getPeriodicDocumentProcessTime() / Main.documentProcessTimeHandler.getPeriodicDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";

			periodicDocumentTimeTable += "<tr>"
					           + "<td>Update Mongo</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getPeriodicMongoUpdateTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getPeriodicMongoUpdateTime() / Main.documentProcessTimeHandler.getPeriodicDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			periodicDocumentTimeTable += "<tr>"
					           + "<td>NER</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getPeriodicNerTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getPeriodicNerTime() / Main.documentProcessTimeHandler.getPeriodicDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			periodicDocumentTimeTable += "<tr>"
					           + "<td>User NER</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getPeriodicUserNerTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getPeriodicUserNerTime() / Main.documentProcessTimeHandler.getPeriodicDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			periodicDocumentTimeTable += "<tr>"
					           + "<td>Geoname List</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getPeriodicNerGeonameTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getPeriodicNerGeonameTime() / Main.documentProcessTimeHandler.getPeriodicDocumentProcessTime() * 100) + "%</td>"
					           + "</tr>";
			
			periodicDocumentTimeTable += "<tr>"
					           + "<td>Geojson</td>"
					           + "<td>" + Main.documentProcessTimeHandler.getPeriodicGeojsonTime() + "</td>"
					           + "<td>" + df.format((double)Main.documentProcessTimeHandler.getPeriodicGeojsonTime() / Main.documentProcessTimeHandler.getPeriodicDocumentProcessTime() * 100)+ "%</td>"
					           + "</tr>";
			
			periodicDocumentTimeTable += "</table>";
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
			response += "<p>Periodic Document Process Time</p>";
			response += periodicDocumentTimeTable;
			response += "<p>Geoname Time Break Down</p>";
			response += geonameTimeTable;
			response += "</body></html>";
		}
		return response;
	}
}

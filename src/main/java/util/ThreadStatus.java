package util;

import java.util.LinkedHashMap;

import org.bson.types.ObjectId;

public class ThreadStatus {
	public static String makeHttpTableHeader(){
		String msg = "<tr>"
				+ "<td>Thread ID</td>"
				+ "<td>System Thread ID</td>"
				+ "<td>Current Time Range</td>"
				+ "<td>Current Object ID Range</td>"
				+ "<td>Number of Documents in Thread</td>"
				+ "<td>Current Object ID</td>"
				+ "<td>Current Insertion Timestamp</td>"
				+ "<td>Current Insertion Time</td>"
				+ "<td>Current Tweet ID</td>"
				+ "<td>Cuurent Status</td>"
				+ "</tr>";
		return msg;
	}
	public int threadId = 0;
	public long systemThreadId = 0;
	public ObjectId currentObjectId = null;
	public long currentInsertionTime = 0;
	public long currentTweetId = 0;
	public TimeRange timeRange = null;
	public int numDocs = 0;
	public boolean isActive = true;
	public boolean isBlocked = false;
	
	public ThreadStatus(int threadId){
		this.threadId = threadId;
	}
	
	public String toString(){
		return "(thread id = " + this.threadId 
				+ ", system thread id = " + this.systemThreadId 
				+ ", current time range =  " + ((this.timeRange != null) ?  this.timeRange.toString() : "empty")
				+ ", current object id range = " + ((this.timeRange != null) ? this.timeRange.toObjectIdString() : "empty")
				+ ", number of documents = " + this.numDocs
				+ ", current object id = " + currentObjectId 
				+ ", current insertion time = " + currentInsertionTime 
				+ ", current tweet id = " + currentTweetId 
				+ ", current status = " + currentStatus()
				+ ")";
	}
	
	public String toThreadIdString(){
		return "(Thread ID: " + this.threadId + ", System Thread ID:" + this.systemThreadId + ")";
	}
	public String toHttpTableRowEntry(){
		String msg = "<tr>"
				+ "<td>" + this.threadId + "</td>"
				+ "<td>" + this.systemThreadId + "</td>"
				+ "<td>" + ((this.timeRange != null) ? this.timeRange.toString() : null) + "</td>"
				+ "<td>" + this.timeRange.toObjectIdString() + "</td>"
				+ "<td>" + this.numDocs + "</td>"
				+ "<td>" + this.currentObjectId + "</td>"
				+ "<td>" + this.currentInsertionTime + "</td>"
				+ "<td>" + TimeUtilities.js_timestampToString(this.currentInsertionTime) + "</td>"
				+ "<td>" + this.currentTweetId + "</td>"
				+ "<td>" + currentStatus() + "</td>"
				+ "</tr>";
		
		return msg;
	}
	public LinkedHashMap<String, Object> toLinkedHashMap(){
		LinkedHashMap<String, Object> rLm = new LinkedHashMap<String, Object>();
		rLm.put("threadId", this.threadId);
		rLm.put("systemThreadId", this.systemThreadId);
		rLm.put("timeRange", (this.timeRange != null) ? this.timeRange.toLinkedHashMap() : null);
		rLm.put("numDocs", this.numDocs);
		rLm.put("currentObjectId", this.currentObjectId.toHexString());
		rLm.put("currentInsertionTime", this.currentInsertionTime);
		rLm.put("currentTweetId", this.currentTweetId);
		rLm.put("currentStatus", currentStatus());
		return rLm;
	}
	private String currentStatus(){
		if(this.isActive){
			if(this.isBlocked){
				return "blocked";
			}
			else{
				return "running";
			}
		}
		else{
			return "inactive";
		}
	}
}

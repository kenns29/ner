package util;

import org.bson.types.ObjectId;

public class ThreadStatus {
	public static String makeHttpTableHeader(){
		String msg = "<tr>"
				+ "<td>Thread ID</td>"
				+ "<td>Current Time Range</td>"
				+ "<td>Current Object ID</td>"
				+ "<td>Current Insertion Timestamp</td>"
				+ "<td>Current Insertion Time</td>"
				+ "<td>Current Tweet ID</td>"
				+ "</tr>";
		return msg;
	}
	public int threadId = 0;
	public ObjectId currentObjectId = null;
	public long currentInsertionTime = 0;
	public long currentTweetId = 0;
	public TimeRange timeRange = null;
	
	public ThreadStatus(int threadId){
		this.threadId = threadId;
	}
	
	public String toString(){
		String msg = "(thread id = " + this.threadId + ", current time range =  " + this.timeRange.toString() + ", current object id = " + currentObjectId + ", " + " current insertion time = " + currentInsertionTime + ", currentTweetId = " + currentTweetId + ")";
		return msg;
	}
	
	public String toHttpTableRowEntry(){
		String msg = "<tr>"
				+ "<td>" + this.threadId + "</td>"
				+ "<td>" + this.timeRange.toString() + "</td>"
				+ "<td>" + this.currentObjectId + "</td>"
				+ "<td>" + this.currentInsertionTime + "</td>"
				+ "<td>" + TimeUtilities.js_timestampToString(this.currentInsertionTime) + "</td>"
				+ "<td>" + this.currentTweetId + "</td>"
				+ "</tr>";
		
		return msg;
	}
}

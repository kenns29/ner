package util;

public class TimeRange {
	public static String makeHttpTableHeader(){
		String msg = "<tr>"
				+ "<td>Start Time</td>"
				+ "<td>End Time</td>"
				+ "<td>Start Timestamp</td>"
				+ "<td>End Timestamp</td>"
				+ "</tr>";
		return msg;
	}
	//Timestamp stored as miliseconds
	public long startTime = 0;
	public long endTime = 0;
	
	TimeRange(){}
	public TimeRange(long js_startTime, long js_endTime){
		this.startTime = js_startTime;
		this.endTime = js_endTime;
	}
	
	public String toString(){
		return "(" + TimeUtilities.js_timestampToString(startTime) + " To " + TimeUtilities.js_timestampToString(endTime) + ")";
	}
	
	public String toHttpTableRowEntry(){
		return "<tr> "
				+ "<td>" + TimeUtilities.js_timestampToString(startTime) + "</td>"
				+ "<td>" + TimeUtilities.js_timestampToString(endTime) + "</td>"
				+ "<td>" + startTime + "</td>"
				+ "<td>" + endTime + "</td>"
				+ "</tr>";
	}
}

package util;

public class TimeRange {
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
}

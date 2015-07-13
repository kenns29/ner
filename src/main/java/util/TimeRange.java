package util;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.bson.types.ObjectId;

import com.mongodb.DBObject;

public class TimeRange implements Comparable{
	public static String makeHttpTableHeader(){
		String msg = "<tr>"
				+ "<td>Start Time</td>"
				+ "<td>End Time</td>"
				+ "<td>Start Timestamp</td>"
				+ "<td>End Timestamp</td>"
				+ "<td>Start Object ID</td>"
				+ "<td>End Object ID</td>"
				+ "<td>Task Type</td>"
				+ "</tr>";
		return msg;
	}
	//Timestamp stored as miliseconds
	public long startTime = 0;
	public long endTime = 0;
	public ObjectId startObjectId = null;
	public ObjectId endObjectId = null;
	public ArrayList<DBObject> mongoObjList = new ArrayList<DBObject>();
	public TaskType taskType = new TaskType();
	TimeRange(){}
	public TimeRange(long js_startTime, long js_endTime){
		this.startTime = js_startTime;
		this.endTime = js_endTime;
		this.startObjectId = TimeUtilities.getObjectIdFromTimestamp(js_startTime);
		this.endObjectId = TimeUtilities.getObjectIdFromTimestamp(js_endTime);
	}
	
	public TimeRange(ObjectId startObjectId, ObjectId endObjectId, ArrayList<DBObject> mongoObjList){
		this.startObjectId = startObjectId;
		this.endObjectId = endObjectId;
		this.startTime = TimeUtilities.getTimestampFromObjectId(startObjectId);
		this.endTime = TimeUtilities.getTimestampFromObjectId(endObjectId);
		this.mongoObjList = mongoObjList;
	}
	
	public TimeRange(ObjectId startObjectId, ObjectId endObjectId, ArrayList<DBObject> mongoObjList, int type){
		this(startObjectId, endObjectId, mongoObjList);
		taskType.setType(type);
	}
	public String toString(){
		return "(" + TimeUtilities.js_timestampToString(startTime) + " To " + TimeUtilities.js_timestampToString(endTime) + ", " + taskType.toString() +  ")";
	}
	
	public String toTimeSting(){
		return "(" + TimeUtilities.js_timestampToString(startTime) + " To " + TimeUtilities.js_timestampToString(endTime) + ")";
	}
	public String toObjectIdString(){
		return "(" + this.startObjectId.toHexString() + " To " + this.endObjectId + ", " + taskType.toString() + ")";
	}
	
	public String toHttpTableRowEntry(){
		return "<tr> "
				+ "<td>" + TimeUtilities.js_timestampToString(startTime) + "</td>"
				+ "<td>" + TimeUtilities.js_timestampToString(endTime) + "</td>"
				+ "<td>" + startTime + "</td>"
				+ "<td>" + endTime + "</td>"
				+ "<td>" + this.startObjectId.toHexString() + "</td>"
				+ "<td>" + this.endObjectId.toHexString() + "</td>"
				+ "<td>" + this.taskType.toString() + "</td>"
				+ "</tr>";
	}
	
	public LinkedHashMap<String, Object> toLinkedHashMap(){
		LinkedHashMap<String, Object> rLm = new LinkedHashMap<String, Object>();
		rLm.put("startTime",this.startTime);
		rLm.put("endTime", this.endTime);
		rLm.put("startObjectId", this.startObjectId.toHexString());
		rLm.put("endObjectId", this.endObjectId.toHexString());
		rLm.put("taskType", this.taskType.toString());
		return rLm;
	}
	@Override
	public int compareTo(Object obj) {
		if(obj instanceof TimeRange){
			TimeRange timeRange = (TimeRange) obj;
			if(this.startTime < timeRange.startTime){
				return -1;
			}
			else if(this.startTime == timeRange.startTime){
				return 0;
			}
			else{
				return 1;
			}
		}
		else{
			return 0;
		}
	}
}

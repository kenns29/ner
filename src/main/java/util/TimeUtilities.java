package util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.bson.types.ObjectId;

public class TimeUtilities {
	
	public static final int dayMillis = 86400000;
	public static final int minutesMillis = 60000;
	public static final int hourMillis = 3600000;
	public static long dayFloor(long timestamp){
		long mod = timestamp % dayMillis;
		return timestamp - mod;
	}
	public static long dayCeiling(long timestamp){
		long mod = timestamp % dayMillis;
		return timestamp + (dayMillis - mod);
	}
	public static long hourFloor(long timestamp){
		long mod = timestamp % hourMillis;
		return timestamp - mod;
	}
	public static long hourCeiling(long timestamp){
		long mod = timestamp % hourMillis;
		return timestamp + (hourMillis - mod);
	}
	public static long minutesFloor(long timestamp){
		long mod = timestamp % minutesMillis;
		return timestamp - mod;
	}
	public static long minutesCeiling(long timestamp){
		long mod = timestamp % minutesMillis;
		return timestamp + (minutesMillis - mod);
	}
	public static ObjectId getObjectIdFromTimestamp(long js_timestamp){
		int seconds = (int) (js_timestamp / 1000);
		String timeHex = Integer.toHexString(seconds);
		if(timeHex.length() < 8){
			String zeros = "";
			for(int i = 0; i < 6 - timeHex.length(); i++){
				zeros += "0";
			}
			timeHex = zeros + timeHex;
		}
		return new ObjectId(timeHex + "0000000000000000");
	}
	
	public static ObjectId getObjectId(long js_timestamp, int machineId, int pid, int serialNumber){
		int seconds = (int) (js_timestamp / 1000);
		String timeHex = Integer.toHexString(seconds);
		String machineIdHex = Integer.toHexString(machineId);
		String pidHex = Integer.toHexString(pid);
		String serialNumberHex = Integer.toHexString(serialNumber);
		
		if(timeHex.length() < 8){
			String zeros = "";
			for(int i = 0; i < 6 - timeHex.length(); i++){
				zeros += "0";
			}
			timeHex = zeros + timeHex;
		}
		if(machineIdHex.length() < 6){
			String zeros = "";
			for(int i = 0; i < 6 - machineIdHex.length(); i++){
				zeros += "0";
			}
			machineIdHex = zeros + machineIdHex;
		}
		if(pidHex.length() < 4){
			String zeros = "";
			for(int i = 0; i < 4 - pidHex.length(); i++){
				zeros += "0";
			}
			pidHex = zeros + pidHex;
		}
		if(serialNumberHex.length() < 6){
			String zeros = "";
			for(int i = 0; i < 6 - serialNumberHex.length(); i++){
				zeros += "0";
			}
			serialNumberHex = zeros + serialNumberHex;
		}
		
		return new ObjectId(timeHex + machineIdHex + pidHex + serialNumberHex);
	}
	
	public static long getTimestampFromObjectId(ObjectId objectId){
		return objectId.getDate().getTime();
	}
//	public static long getJsTimestampFromObjectId(ObjectId objectId){
//		return objectId.getDate().getTime() * 1000;
//	}
	public static String js_timestampToString(long js_timestamp){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss, z");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		return sdf.format(new Date(js_timestamp));
	}
	
}

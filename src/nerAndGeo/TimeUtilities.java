package nerAndGeo;

import org.bson.types.ObjectId;

public class TimeUtilities {
	public static final int dayMillis = 86400000;
	public static long dayFloor(long timestamp){
		long mod = timestamp % dayMillis;
		return timestamp - mod;
	}
	public static long dayCeiling(long timestamp){
		long mod = timestamp % dayMillis;
		return timestamp + (dayMillis - mod);
	}
	
	public static ObjectId getObjectIdFromTimestamp(long js_timestamp){
		int seconds = (int) (js_timestamp / 1000);
		String hex = Integer.toHexString(seconds);
		return new ObjectId(hex + "0000000000000000");
	}
}

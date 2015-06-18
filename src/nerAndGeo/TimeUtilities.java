package nerAndGeo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

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
	
	public static String js_timestampToString(long js_timestamp){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss, z");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		return sdf.format(new Date(js_timestamp));
	}
	
}

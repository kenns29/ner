package nerAndGeo;

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
}

package timer.timeObj;

public class GeonameTime {
	private long geonameCacheGetTime = 0;
	private long geonameCachePutTime = 0;
	private long nullCacheCheckTime = 0;
	private long nullCachePutTime = 0;
	private long geonameTime = 0;
	private long totalGeonameTime = 0;
	public long getGeonameCacheGetTime() {
		return geonameCacheGetTime;
	}
	public void setGeonameCacheGetTime(long geonameCacheGetTime) {
		this.geonameCacheGetTime = geonameCacheGetTime;
	}
	public long getGeonameCachePutTime() {
		return geonameCachePutTime;
	}
	public void setGeonameCachePutTime(long geonameCachePutTime) {
		this.geonameCachePutTime = geonameCachePutTime;
	}
	public long getNullCacheCheckTime() {
		return nullCacheCheckTime;
	}
	public void setNullCacheCheckTime(long nullCacheCheckTime) {
		this.nullCacheCheckTime = nullCacheCheckTime;
	}
	public long getNullCachePutTime() {
		return nullCachePutTime;
	}
	public void setNullCachePutTime(long nullCachePutTime) {
		this.nullCachePutTime = nullCachePutTime;
	}
	public long getGeonameTime() {
		return geonameTime;
	}
	public void setGeonameTime(long geonameTime) {
		this.geonameTime = geonameTime;
	}
	public long getTotalGeonameTime() {
		return totalGeonameTime;
	}
	public void setTotalGeonameTime(long totalGeonameTime) {
		this.totalGeonameTime = totalGeonameTime;
	}
	
	
}

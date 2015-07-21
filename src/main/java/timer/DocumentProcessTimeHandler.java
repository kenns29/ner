package timer;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import nerAndGeo.Database;
import nerAndGeo.Main;

public class DocumentProcessTimeHandler {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	
	public static int documentCountInterval = 1000;
	public Database periodicDocumentProcessTimeHost = null;
	public DB periodicDocumentProcessTimeDB = null;
	public DBCollection periodicDocumentProcessTimeColl = null;
	
	public Object lockObjectDocumentProcessTime = new Object();
	private long totalDocumentProcessTime = 0;
	private long totalMongoUpdateTime = 0;
	private long totalNerTime = 0;
	private long totalUserNerTime = 0;
	private long totalNerGeonameTime = 0;
	private long totalGeojsonTime = 0;
	
	private long periodicDocumentProcessTime = 0;
	private long periodicMongoUpdateTime = 0;
	private long periodicNerTime = 0;
	private long periodicUserNerTime = 0;
	private long periodicNerGeonameTime = 0;
	private long periodicGeojsonTime = 0;
	
	public long documentProcessTime = 0;
	public long mongoUpdateTime = 0;
	public long nerTime = 0;
	public long userNerTime = 0;
	public long nerGeonameTime = 0;
	public long geojsonTime = 0;
	
	public Object lockObjectGeonameTime = new Object();
	private long geonameCacheGetTime = 0;
	private long geonameCachePutTime = 0;
	private long nullCacheCheckTime = 0;
	private long nullCachePutTime = 0;
	private long geonameTime = 0;
	private long totalGeonameTime = 0;
	
	public long tempGeonameCacheGetTime = 0;
	public long tempGeonameCachePutTime = 0;
	public long tempNullCacheCheckTime = 0;
	public long tempNullCachePutTime = 0;
	public long tempGeonameTime = 0;
	public long tempTotalGeonameTime = 0;
	
	public DocumentProcessTimeHandler(){
		try {
			this.periodicDocumentProcessTimeHost = new Database(Main.configPropertyValues.periodicDocumentProcessTimeHost, Main.configPropertyValues.periodicDocumentProcessTimePort);
		} catch (UnknownHostException e) {
			HIGH_PRIORITY_LOGGER.error("Document Process host not found", e);
		}
		this.periodicDocumentProcessTimeDB = this.periodicDocumentProcessTimeHost.getDatabase(Main.configPropertyValues.periodicDocumentProcessTimeDatabase);
		this.periodicDocumentProcessTimeColl = this.periodicDocumentProcessTimeDB.getCollection(Main.configPropertyValues.periodicDocumentProcessTimeCollection);
		this.periodicDocumentProcessTimeColl.remove(new BasicDBObject());
	}
	
	public void updateTotalTime(){
		if(this.totalDocumentProcessTime >= Long.MAX_VALUE - this.documentProcessTime - 5000){
			this.totalDocumentProcessTime = 0;
			this.totalUserNerTime = 0;
			this.totalNerTime = 0;
			this.totalNerGeonameTime = 0;
			this.totalGeojsonTime = 0;
			this.totalMongoUpdateTime = 0;
		}
		this.totalDocumentProcessTime += this.documentProcessTime;
		this.totalUserNerTime += this.userNerTime;
		this.totalNerTime += this.nerTime;
		this.totalNerGeonameTime += this.nerGeonameTime;
		this.totalGeojsonTime += this.geojsonTime;
		this.totalMongoUpdateTime += this.mongoUpdateTime;
	}
	
	public void incPeriodicTime(){
		this.periodicDocumentProcessTime += this.documentProcessTime;
		this.setPeriodicUserNerTime(this.getPeriodicUserNerTime() + this.userNerTime);
		this.periodicNerTime += this.nerTime;
		this.periodicNerGeonameTime += this.nerGeonameTime;
		this.periodicGeojsonTime += this.geojsonTime;
		this.periodicMongoUpdateTime += this.mongoUpdateTime;
	}
	
	public void resetPeriodicTime(){
		this.periodicDocumentProcessTime = 0;
		this.periodicUserNerTime = 0;
		this.periodicNerTime = 0;
		this.periodicNerGeonameTime = 0;
		this.periodicGeojsonTime = 0;
		this.periodicMongoUpdateTime = 0;
	}
	
	public void updateGeonameTotalTime(){
		synchronized(this.lockObjectGeonameTime){
			if(this.totalGeonameTime >= Long.MAX_VALUE - tempTotalGeonameTime - 5000){
				this.geonameCacheGetTime = 0;
				this.geonameCachePutTime = 0;
				this.geonameTime = 0;
				this.nullCacheCheckTime = 0;
				this.nullCachePutTime = 0;
				this.totalGeonameTime = 0;
			}
			this.geonameCacheGetTime += tempGeonameCacheGetTime;
			this.geonameCachePutTime += tempGeonameCachePutTime;
			this.geonameTime += tempGeonameTime;
			this.nullCacheCheckTime += tempNullCacheCheckTime;
			this.nullCachePutTime += tempNullCachePutTime;
			this.totalGeonameTime += tempTotalGeonameTime;
		}
	}

	
	public void updateMongoForPeriodicDocumentProcessTime(){
		BasicDBObject query = new BasicDBObject("docCount", Main.documentCount);
		
		BasicDBObject update = new BasicDBObject("docCount", Main.documentCount)
				.append("documentProcessTime", this.periodicDocumentProcessTime)
				.append("userNerTime", this.periodicUserNerTime)
				.append("nerTime", this.periodicNerTime)
				.append("geonameTime", this.periodicNerGeonameTime)
				.append("geojsonTime", this.periodicGeojsonTime)
				.append("mongoUpdateTime", this.periodicMongoUpdateTime);
		
		this.periodicDocumentProcessTimeColl.update(query, update, true, false);
	}
	/////////////////////////
	///Getter and Setter/////
	/////////////////////////
	public long getPeriodicUserNerTime() {
		return periodicUserNerTime;
	}

	public void setPeriodicUserNerTime(long periodicUserNerTime) {
		this.periodicUserNerTime = periodicUserNerTime;
	}

	public long getTotalDocumentProcessTime() {
		return totalDocumentProcessTime;
	}

	public void setTotalDocumentProcessTime(long totalDocumentProcessTime) {
		this.totalDocumentProcessTime = totalDocumentProcessTime;
	}

	public long getTotalMongoUpdateTime() {
		return totalMongoUpdateTime;
	}

	public void setTotalMongoUpdateTime(long totalMongoUpdateTime) {
		this.totalMongoUpdateTime = totalMongoUpdateTime;
	}

	public long getTotalNerTime() {
		return totalNerTime;
	}

	public void setTotalNerTime(long totalNerTime) {
		this.totalNerTime = totalNerTime;
	}

	public long getTotalUserNerTime() {
		return totalUserNerTime;
	}

	public void setTotalUserNerTime(long totalUserNerTime) {
		this.totalUserNerTime = totalUserNerTime;
	}

	public long getTotalNerGeonameTime() {
		return totalNerGeonameTime;
	}

	public void setTotalNerGeonameTime(long totalNerGeonameTime) {
		this.totalNerGeonameTime = totalNerGeonameTime;
	}

	public long getTotalGeojsonTime() {
		return totalGeojsonTime;
	}

	public void setTotalGeojsonTime(long totalGeojsonTime) {
		this.totalGeojsonTime = totalGeojsonTime;
	}

	public long getPeriodicDocumentProcessTime() {
		return periodicDocumentProcessTime;
	}

	public void setPeriodicDocumentProcessTime(long periodicDocumentProcessTime) {
		this.periodicDocumentProcessTime = periodicDocumentProcessTime;
	}

	public long getPeriodicMongoUpdateTime() {
		return periodicMongoUpdateTime;
	}

	public void setPeriodicMongoUpdateTime(long periodicMongoUpdateTime) {
		this.periodicMongoUpdateTime = periodicMongoUpdateTime;
	}

	public long getPeriodicNerTime() {
		return periodicNerTime;
	}

	public void setPeriodicNerTime(long periodicNerTime) {
		this.periodicNerTime = periodicNerTime;
	}

	public long getPeriodicNerGeonameTime() {
		return periodicNerGeonameTime;
	}

	public void setPeriodicNerGeonameTime(long periodicNerGeonameTime) {
		this.periodicNerGeonameTime = periodicNerGeonameTime;
	}

	public long getPeriodicGeojsonTime() {
		return periodicGeojsonTime;
	}

	public void setPeriodicGeojsonTime(long periodicGeojsonTime) {
		this.periodicGeojsonTime = periodicGeojsonTime;
	}

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

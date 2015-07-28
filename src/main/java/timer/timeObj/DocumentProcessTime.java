package timer.timeObj;

public class DocumentProcessTime {
	private long documentProcessTime = 0;
	private long mongoUpdateTime = 0;
	private long nerTime = 0;
	private long userNerTime = 0;
	private long nerGeonameTime = 0;
	private long geojsonTime = 0;
	public long getDocumentProcessTime() {
		return documentProcessTime;
	}
	public void setDocumentProcessTime(long documentProcessTime) {
		this.documentProcessTime = documentProcessTime;
	}
	public long getMongoUpdateTime() {
		return mongoUpdateTime;
	}
	public void setMongoUpdateTime(long mongoUpdateTime) {
		this.mongoUpdateTime = mongoUpdateTime;
	}
	public long getNerTime() {
		return nerTime;
	}
	public void setNerTime(long nerTime) {
		this.nerTime = nerTime;
	}
	public long getUserNerTime() {
		return userNerTime;
	}
	public void setUserNerTime(long userNerTime) {
		this.userNerTime = userNerTime;
	}
	public long getNerGeonameTime() {
		return nerGeonameTime;
	}
	public void setNerGeonameTime(long nerGeonameTime) {
		this.nerGeonameTime = nerGeonameTime;
	}
	public long getGeojsonTime() {
		return geojsonTime;
	}
	public void setGeojsonTime(long geojsonTime) {
		this.geojsonTime = geojsonTime;
	}
	
}

package configProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.bson.types.ObjectId;

import util.CollUtilities;

import com.mongodb.DBCollection;

public class ConfigPropertyValues {
	public String host = null;
	public int port = 0;
	public String db = null;
	public String coll = null;
	
	public String nerInputField = null;
	public int catID = 0;

	public String outputHost = null;
	public int outputPort = 0;
	public String outputDatabase = null;
	public String outputCollection = null;
	
	public int outputOption = 0;
	public String nerOutputField = null;
	public String geojsonListOutputField = null;
	public String geonameOutputField = null;
	
	public boolean userNer = true;
	public String userNerOutputField = null;
	
	public int core = 0;
	public int queueSize = 4;
	public boolean parallel = false;
	
	//0: split task by insertion time
	//1: split task by creation time
	public int useInsertionOrCreationTime = 0;
	
	public int splitOption = 1;
	public long splitIntervalInMillis = 0;
	public int numDocsInThread = 0;
	public boolean geoname = false;
	
	public boolean useTimeLimit = false;
	public long startTime = -1;
	public long endTime = -1;
	public boolean useObjectIdLimit = false;
	public ObjectId startObjectId = null;
	public ObjectId endObjectId = null;
	public boolean stopAtEnd = true;
	
	public boolean useGeonameCache = true;
	public String cacheHost = null;
	public int cachePort = 0;
	public String cacheDatabase = null;
	public String cacheCollection = null;
	
	public String nullCacheHost = null;
	public int nullCachePort = 0;
	public String nullCacheDatabase = null;
	public String nullCacheCollection = null;
	
	public String statusHttpServerHost = null;
	public int statusHttpServerPort = 0;

	public String retryCacheHost = null;
	public int retryCachePort = 0;
	public String retryCacheDatabase = null;
	public String retryCacheCollection = null;
	
	public String errorCacheHost = null;
	public int errorCachePort = 0;
	public String errorCacheDatabase = null;
	public String errorCacheCollection = null;
	
	public ConfigPropertyValues() throws IOException{
		this("config.properties");
	}
	public ConfigPropertyValues(String propFileName) throws IOException{
		getPropValues(propFileName);
	}
	public void getPropValues() throws IOException{
		getPropValues("config.properties");
	}
	@SuppressWarnings("unused")
	public void getPropValues(String propFileName) throws IOException{
		Properties prop = new Properties();
		File file = new File(propFileName);
		FileInputStream inputStream= new FileInputStream(file);
		//InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		
		if(inputStream != null){
			prop.load(inputStream);
		}
		else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}
		
		host = prop.getProperty("host");
		port = Integer.valueOf(prop.getProperty("port"));
		db = prop.getProperty("db");
		coll = prop.getProperty("coll");
		
		nerInputField = prop.getProperty("nerInputField");
		
		catID = Integer.valueOf(prop.getProperty("catID"));
		
		outputHost = prop.getProperty("outputHost");
		outputPort = Integer.valueOf(prop.getProperty("outputPort"));
		outputDatabase = prop.getProperty("outputDatabase");
		outputCollection = prop.getProperty("outputCollection");
		
		outputOption = Integer.valueOf(prop.getProperty("outputOption"));
		nerOutputField = prop.getProperty("nerOutputField");
		geojsonListOutputField = prop.getProperty("geojsonListOutputField");
		geonameOutputField = prop.getProperty("geonameOutputField");
		
		userNer = Boolean.parseBoolean(prop.getProperty("userNer"));
		userNerOutputField = prop.getProperty("userNerOutputField");
		
		core = Integer.valueOf(prop.getProperty("core"));
		queueSize = Integer.valueOf(prop.getProperty("queueSize"));
		parallel = Boolean.parseBoolean(prop.getProperty("parallel"));
		useInsertionOrCreationTime = Integer.valueOf(prop.getProperty("useInsertionOrCreationTime"));
		
		splitOption = Integer.valueOf(prop.getProperty("splitOption"));
		splitIntervalInMillis = Long.valueOf(prop.getProperty("splitIntervalInMillis"));
		numDocsInThread = Integer.valueOf(prop.getProperty("numDocsInThread"));
		
		geoname = Boolean.parseBoolean(prop.getProperty("geoname"));
				
		useTimeLimit = Boolean.parseBoolean(prop.getProperty("useTimeLimit"));
		String startTimeStr = prop.getProperty("startTime");
		String endTimeStr = prop.getProperty("endTime");
		if(!startTimeStr.equals("none")){
			startTime = Long.valueOf(prop.getProperty("startTime")).longValue();
		}
		else{
			startTime = -1;
		}
		
		if(!endTimeStr.equals("none")){
			endTime = Long.valueOf(prop.getProperty("endTime")).longValue();
		}
		else{
			endTime = -1;
		}
		
		
		useObjectIdLimit = Boolean.parseBoolean(prop.getProperty("useObjectIdLimit"));
		String startObjectIdStr = prop.getProperty("startObjectId");
		String endObjectIdStr = prop.getProperty("endObjectId");
		
		if(!startObjectIdStr.equals("none")){
			startObjectId = new ObjectId(startObjectIdStr);
		}
		
		if(!endObjectIdStr.equals("none")){
			endObjectId = new ObjectId(endObjectIdStr);
		}
		
		stopAtEnd = Boolean.parseBoolean(prop.getProperty("stopAtEnd"));
		
		cacheHost = prop.getProperty("cacheHost");
		cachePort = Integer.valueOf(prop.getProperty("cachePort"));
		useGeonameCache = Boolean.parseBoolean(prop.getProperty("useGeonameCache"));
		cacheDatabase = prop.getProperty("cacheDatabase");
		cacheCollection = prop.getProperty("cacheCollection");
		
		nullCacheHost = prop.getProperty("nullCacheHost");
		nullCachePort = Integer.valueOf(prop.getProperty("nullCachePort"));
		nullCacheDatabase = prop.getProperty("nullCacheDatabase");
		nullCacheCollection = prop.getProperty("nullCacheCollection");
				
		statusHttpServerHost = prop.getProperty("statusHttpServerHost");
		statusHttpServerPort = Integer.valueOf(prop.getProperty("statusHttpServerPort"));
		
		retryCacheHost = prop.getProperty("retryCacheHost");
		retryCachePort = Integer.valueOf(prop.getProperty("retryCachePort"));
		retryCacheDatabase = prop.getProperty("retryCacheDatabase");
		retryCacheCollection = prop.getProperty("retryCacheCollection");
		
		errorCacheHost = prop.getProperty("errorCacheHost");
		errorCachePort = Integer.valueOf(prop.getProperty("errorCachePort")).intValue();
		errorCacheDatabase = prop.getProperty("errorCacheDatabase");
		errorCacheCollection = prop.getProperty("errorCacheCollection");
	}
	
	public void initStartEnd(DBCollection coll){
		if(this.startTime < 0){
			if(this.useInsertionOrCreationTime == 0){
				this.startTime = CollUtilities.minInsertionTime(coll);
			}
			else{
				this.startTime = CollUtilities.minTime(coll);
			}
		}
		
		if(this.endTime < 0){
			if(this.useInsertionOrCreationTime == 0){
				this.endTime = CollUtilities.maxInsertionTime(coll);
			}
			else{
				this.endTime = CollUtilities.maxTime(coll);
			}
		}
		
		if(this.startObjectId == null){
			this.startObjectId = CollUtilities.minObjectId(coll);
		}
		
		if(this.endObjectId == null){
			this.endObjectId = CollUtilities.maxObjectId(coll);
		}
	}
}

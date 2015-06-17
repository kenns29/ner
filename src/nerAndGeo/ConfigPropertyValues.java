package nerAndGeo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigPropertyValues {
	public String host = null;
	public int port = 0;
	public String db = null;
	public String coll = null;
	public String nerInputField = null;
	public String nerOutputField = null;
	
	public int catID = 0;
	
	public int core = 0;
	
	public boolean parallel = false;
	
	//0: split task by insertion time
	//1: split task by creation time
	public int parallelFlag = 0;
	
	public boolean ner = false;
	public boolean geoname = false;
	public boolean geocoding = false;
	
	public boolean useTimeLimit = false;
	public long startTime = 0;
	public long endTime = 0;
	
	public boolean useDocLimit = false;
	public int docLimit = 0;
	
	public boolean useCache = true;
	public String cacheHost = null;
	public int cachePort = 0;
	public String cacheDatabase = null;
	public String cacheCollection = null;
	public int cacheLimit = -1;
	
	public ConfigPropertyValues() throws IOException{
		this("config.properties");
	}
	public ConfigPropertyValues(String propFileName) throws IOException{
		getPropValues(propFileName);
	}
	public void getPropValues() throws IOException{
		getPropValues("config.properties");
	}
	public void getPropValues(String propFileName) throws IOException{
		Properties prop = new Properties();		
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		
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
		nerOutputField = prop.getProperty("nerOutputField");
		
		catID = Integer.valueOf(prop.getProperty("catID"));
		core = Integer.valueOf(prop.getProperty("core"));
		
		parallel = Boolean.parseBoolean(prop.getProperty("parallel"));
		parallelFlag = Integer.valueOf(prop.getProperty("parallelFlag"));
		
		ner = Boolean.parseBoolean(prop.getProperty("ner"));
		geoname = Boolean.parseBoolean(prop.getProperty("geoname"));
		geocoding = Boolean.parseBoolean(prop.getProperty("geocoding"));
		
		useTimeLimit = Boolean.parseBoolean(prop.getProperty("useTimeLimit"));
		String startTimeStr = prop.getProperty("startTime");
		String endTimeStr = prop.getProperty("endTime");
		if(!startTimeStr.equals("none")){
			startTime = Long.getLong(prop.getProperty("startTime"));
		}
		
		if(!endTimeStr.equals("none")){
			endTime = Long.getLong(prop.getProperty("endTime"));
		}
		
		useDocLimit = Boolean.parseBoolean(prop.getProperty("useDocLimit"));
		docLimit = Integer.valueOf(prop.getProperty("docLimit"));
		
		cacheHost = prop.getProperty("cacheHost");
		cachePort = Integer.valueOf(prop.getProperty("cachePort"));
		useCache = Boolean.parseBoolean(prop.getProperty("useCache"));
		cacheDatabase = prop.getProperty("cacheDatabase");
		cacheCollection = prop.getProperty("cacheCollection");
		cacheLimit = Integer.valueOf(prop.getProperty("cacheLimit"));
		
	}
}

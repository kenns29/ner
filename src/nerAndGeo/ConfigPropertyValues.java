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
	public String inputField = null;
	public String outputField = null;
	public int core = 0;
	
	public boolean ner = false;
	public boolean geoname = false;
	public boolean geocoding = false;
	
	public boolean useTimeLimit = false;
	public long startTime = 0;
	public long endTime = 0;
	
	public boolean useDocLimit = false;
	public int docLimit = 0;
	
	public void getPropValues() throws IOException{
		Properties prop = new Properties();
		String propFileName = "config.properties";
		
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
		inputField = prop.getProperty("inputField");
		outputField = prop.getProperty("outputField");
		
		core = Integer.valueOf(prop.getProperty("core"));
		
		ner = Boolean.getBoolean(prop.getProperty("ner"));
		geoname = Boolean.getBoolean(prop.getProperty("geoname"));
		geocoding = Boolean.getBoolean(prop.getProperty("geocoding"));
		
		useTimeLimit = Boolean.getBoolean(prop.getProperty("useTimeLimit"));
		startTime = Long.getLong(prop.getProperty("startTime"));
		endTime = Long.getLong(prop.getProperty("endTime"));
		
		useDocLimit = Boolean.getBoolean(prop.getProperty("useDocLimit"));
		docLimit = Integer.valueOf(prop.getProperty("docLimit"));
	}
}

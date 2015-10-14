package nerAndGeo;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import util.CollUtilities;
import util.CountRunnable;
import util.TimeRange;
import version.VersionControl;

import com.mongodb.DB;
import com.mongodb.DBCollection;

import configProperties.ConfigPropertyValues;
import timer.DocumentProcessTimeHandler;

public class Main {
	public static final int VERSION_MAJOR = 1;
	public static final int VERSION_MINOR = 2;
	public static final int BUILD_ID = 2;

	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static BlockingQueue<TimeRange> queue = null;
	
	public static VersionControl versionControl =
		new VersionControl(VERSION_MAJOR + "." + VERSION_MINOR + "." + BUILD_ID,
			"2015-10-12");
	public static ConfigPropertyValues configPropertyValues = null;
	
	public static long totalDocuments = -1;
	public static AtomicInteger documentCount = new AtomicInteger(0);
	public static AtomicInteger timelyDocCount = new AtomicInteger(0);
	public static AtomicInteger lastTimelyDocCount = new AtomicInteger(0);
	public static AtomicInteger textEntitiesDocCount = new AtomicInteger(0);
	public static AtomicInteger userEntitiesDocCount = new AtomicInteger(0);
	
	public static long mainStartTime = System.currentTimeMillis();
	public static long mainPreTime = System.currentTimeMillis();
	
	public static Object lockObjectThreadFinishCount = new Object();
	public static AtomicInteger threadFinishCount = new AtomicInteger(0);
	public static AtomicInteger taskMangerFinishCount = new AtomicInteger(0);
	public static long totalThreadFinishedTime = 0;
	public static long totalTaskManagerFinishedTime = 0;
	
	public static DocumentProcessTimeHandler documentProcessTimeHandler = null;
	
	public static Object lockObjectRetryCache = new Object();
	public static AtomicInteger retryCacheCount = new AtomicInteger(0);
	public static Database retryCacheHost = null; 
	public static DB retryCacheDB = null;
	public static DBCollection retryCacheColl = null;
	public static boolean retryCacheAvailable = true;
	
	public static Object lockObjectGeonameServiceChecker = new Object();
	public static boolean geonameServiceAvailable = true;
	
	public static Database errorCacheHost = null;
	public static DB errorCacheDB = null;
	public static DBCollection errorCacheColl = null;
	
	public static Thread geonameServiceCheckerThread = new Thread(new GeonameServiceChecker("UK"));
	public static DBCollection mainColl = null;
	public static DBCollection outputColl = null;
	static{
		try {
			configPropertyValues = new ConfigPropertyValues("config.properties");
			queue = new ArrayBlockingQueue<TimeRange>(configPropertyValues.queueSize);
			Properties props = new Properties();
			InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("log4j.properties");
			props.load(inputStream);
			PropertyConfigurator.configure(props);
		} catch (IOException e) {
			HIGH_PRIORITY_LOGGER.fatal("Start Error", e);
		}
		
		try {
			retryCacheHost = new Database(Main.configPropertyValues.retryCacheHost, Main.configPropertyValues.retryCachePort);
		} catch (UnknownHostException e) {
			HIGH_PRIORITY_LOGGER.fatal("retry cache host error", e);
		}
		retryCacheDB = retryCacheHost.getDatabase(Main.configPropertyValues.retryCacheDatabase);
		retryCacheColl = retryCacheDB.getCollection(Main.configPropertyValues.retryCacheCollection);
		
		try {
			errorCacheHost = new Database(Main.configPropertyValues.errorCacheHost, Main.configPropertyValues.errorCachePort);
		} catch (UnknownHostException e) {
			HIGH_PRIORITY_LOGGER.fatal("error cache host error", e);
		}
		errorCacheDB = errorCacheHost.getDatabase(Main.configPropertyValues.errorCacheDatabase);
		errorCacheColl = errorCacheDB.getCollection(Main.configPropertyValues.errorCacheCollection);
		documentProcessTimeHandler = new DocumentProcessTimeHandler();
	}
	
	public static void main(String[] args) throws Exception {		
		//Use the config file
		if(args.length >= 1){
			if(args[0].equals("--version")){
				System.out.println(versionControl.toLog());
				System.exit(0);
			}
			else{
				System.out.println("Invalid argument.");
				System.exit(1);
			}
			
		}
		LOGGER.info("\n" + versionControl.toLog());
		Database database = new Database(configPropertyValues.host, configPropertyValues.port);
		DB db = database.getDatabase(configPropertyValues.db);
		DBCollection coll = db.getCollection(configPropertyValues.coll);
		configPropertyValues.initStartEnd(coll);
		Main.mainColl = coll;
		if(Main.configPropertyValues.stopAtEnd){
			new Thread(new CountRunnable()).start();
		}
		
		Database outputDatabase = new Database(Main.configPropertyValues.outputHost, Main.configPropertyValues.outputPort);
		DB outputDB = outputDatabase.getDatabase(Main.configPropertyValues.outputDatabase);
		Main.outputColl = outputDB.getCollection(Main.configPropertyValues.outputCollection);
		
		if(configPropertyValues.parallel){
			if(configPropertyValues.useTimeLimit){
				NER.parallelNER(coll, Main.configPropertyValues.nerInputField, Main.configPropertyValues.startTime, Main.configPropertyValues.endTime, Main.queue);
			}
			else if(configPropertyValues.useObjectIdLimit){
				NER.parallelNER(coll, Main.configPropertyValues.nerInputField, Main.configPropertyValues.startObjectId, Main.configPropertyValues.endObjectId, Main.queue);
			}
			else{
				NER.parallelNER(coll, Main.configPropertyValues.nerInputField, queue);
			}
		}
		else{
			NER.insertNer(coll, configPropertyValues.nerInputField);
			
		}
		//database.close();
	}
}

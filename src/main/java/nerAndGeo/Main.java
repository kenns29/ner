package nerAndGeo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import util.TimeRange;
import version.VersionControl;

import com.mongodb.DB;
import com.mongodb.DBCollection;

import configProperties.ConfigPropertyValues;

public class Main {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	
	public static BlockingQueue<TimeRange> queue = null;
	
	public static VersionControl versionControl = new VersionControl("1.0.0", "07-08-2015");
	public static ConfigPropertyValues configPropertyValues = null;
	
	public static AtomicInteger documentCount = new AtomicInteger(0);
	public static AtomicInteger timelyDocCount = new AtomicInteger(0);
	public static AtomicInteger textEntitiesDocCount = new AtomicInteger(0);
	public static AtomicInteger userEntitiesDocCount = new AtomicInteger(0);
	public static long mainPreTime = System.currentTimeMillis();
	
	static{
		try {
			configPropertyValues = new ConfigPropertyValues("config.properties");
			queue = new ArrayBlockingQueue<TimeRange>(6);
			Properties props = new Properties();
			InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("log4j.properties");
			props.load(inputStream);
			PropertyConfigurator.configure(props);
		} catch (IOException e) {
			e.printStackTrace();
		}
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

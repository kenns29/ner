package nerAndGeo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.PropertyConfigurator;

import util.TimeRange;

import com.mongodb.DB;
import com.mongodb.DBCollection;

import configProperties.ConfigPropertyValues;

public class Main {
	public static BlockingQueue<TimeRange> queue = new ArrayBlockingQueue<TimeRange>(4);
	public static ConfigPropertyValues configPropertyValues = null;
	public static int documentCount = 0;
	public static long mainPreTime = System.currentTimeMillis();
	static{
		try {
			configPropertyValues = new ConfigPropertyValues("config.properties");
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
		Database database = new Database(configPropertyValues.host, configPropertyValues.port);
		DB db = database.getDatabase(configPropertyValues.db);
		DBCollection coll = db.getCollection(configPropertyValues.coll);
		
		if(configPropertyValues.parallel){
			if(configPropertyValues.ner){
				if(configPropertyValues.useTimeLimit){
					NER.parallelNER(coll, Main.configPropertyValues.nerInputField, Main.configPropertyValues.startTime, Main.configPropertyValues.endTime, Main.queue);
				}
				else{
					NER.parallelNER(coll, Main.configPropertyValues.nerInputField, queue);
				}
			}
		}
		else{
			if(configPropertyValues.ner){
				NER.insertNer(coll, configPropertyValues.nerInputField);
			}
		}
		//database.close();
	}
}

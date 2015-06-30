package nerAndGeo;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import util.TimeRange;

import com.mongodb.DB;
import com.mongodb.DBCollection;

public class Main {
	public static ConfigPropertyValues configPropertyValues = null;
	public static int documentCount = 0;
	static{
		try {
			configPropertyValues = new ConfigPropertyValues("config.properties");
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
				BlockingQueue<TimeRange> queue = new ArrayBlockingQueue<TimeRange>(Main.configPropertyValues.core); 
				if(configPropertyValues.useTimeLimit){
					NER.parallelNER(coll, Main.configPropertyValues.nerInputField, Main.configPropertyValues.startTime, Main.configPropertyValues.endTime, queue);
				}
				else{
					NER.parallelNER(coll, configPropertyValues.nerInputField, queue);
				}
			}
		}
		else{
			if(configPropertyValues.ner){
				NER.insertNer(coll, configPropertyValues.nerInputField);
			}
		}
		database.close();
	}
}

package nerAndGeo;

import java.io.IOException;
import java.util.Properties;
import org.json.simple.JSONArray;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

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
				if(configPropertyValues.useTimeLimit){
					NER.parallelNer(coll, configPropertyValues.nerInputField, configPropertyValues.startTime, configPropertyValues.endTime);
				}
				else{
					
					NER.parallelNer(coll, configPropertyValues.nerInputField);
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

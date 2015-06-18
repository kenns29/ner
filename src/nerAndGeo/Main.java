package nerAndGeo;

import java.io.IOException;
import java.util.Properties;
import org.json.simple.JSONArray;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class Main {
//	public static Properties NLPprops = new Properties();
//	public static StanfordCoreNLP pipeline = null;
	public static ConfigPropertyValues configPropertyValues = null;
	static{
		//NLPprops.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		//pipeline = new StanfordCoreNLP(NLPprops);
		try {
			configPropertyValues = new ConfigPropertyValues("config.properties");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
//		Database database0 = new Database("fsdb1.dtn.asu.edu", 27017);
//		Database database1 = new Database("vaderserver0.cidse.dhcp.asu.edu", 27017);
//		Database database2 = new Database("fssand1.dtn.asu.edu", 27888);
//		//Database database = new Database("localhost", 27017);
//		DB dbTweettracker = database0.getDatabase("tweettracker");
//		DB dbRSS = database0.getDatabase("foresight");
//		DB dbACLED = database0.getDatabase("conflicts");
//		
//		DB dbNigeriaTweets = database1.getDatabase("NigeriaTweets");
//		DB dbTest = database2.getDatabase("NigeriaTweets");
//		
//		DBCollection sentenceColl = dbRSS.getCollection("sentence");
//		DBCollection tweetsColl = dbTweettracker.getCollection("tweets");
//		DBCollection ACLEDColl = dbACLED.getCollection("ACLEDfull");
//		
//		DBCollection NigeriaColl = dbNigeriaTweets.getCollection("tweets");
//		
//		DBCollection testColl = dbTest.getCollection("tweets");
		
		//Use the config file
		if(args.length == 0){
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
		}
		//use the parameters
//		else if(args[0].equals("-tweets")){
//			if(args[1].equals("-ner")){
//				if(args.length >= 3){
//					Integer catID = Integer.parseInt(args[2]);
//					BasicDBObject query = new BasicDBObject("cat", catID)
//					.append("ner", null);
//					
//					NER.insertNer(tweetsColl, "text", query);
//				}
//				else{
//					NER.insertNer(tweetsColl, "text");
//				}
//			}
//			else if(args[1].equals("-location")){
//				if(args.length >= 3){
//					Integer catID = Integer.parseInt(args[2]);
//					BasicDBObject query = new BasicDBObject("cat", catID)
//					.append("ner", new BasicDBObject("$ne", null));
//					
//					GeoCoding.insertLocation(tweetsColl, query);
//				}
//				else{
//					GeoCoding.insertLocation(tweetsColl);
//				}
//			}
//			else{
//				GeoCoding.insertCoord(tweetsColl);
//			}
//		}
//		else if(args[0].equals("-rss")){
//			if(args[1].equals("-ner")){
//				NER.insertNer(sentenceColl, "sentence");
//			}
//			else if(args[1].equals("-location")){
//				GeoCoding.insertLocation(sentenceColl);
//			}
//			else if(args[1].equals("-geoname")){
//				//insertGeonamesWithException(sentenceColl);
//				
//				Geoname.insertGeoNames(sentenceColl);
//			}
//			else{
//				GeoCoding.insertCoord(sentenceColl);
//			}
//		}
//		else if(args[0].equals("-NigeriaTweets")){
//			if(args[1].equals("-ner")){
//				NER.insertNer(NigeriaColl, "text");
//			}
//			else if(args[1].equals("-location")){
//				GeoCoding.insertLocation(NigeriaColl);
//			}
//			else{
//				
//			}
//		}
//		else if(args[0].equals("-testPar")){
//			if(args[1].equals("-ner")){
//				NER.parallelNer(testColl, "text");
//			}
//		}
//		else if(args[0].equals("-test")){
//			String text = "Yesterday 's meeting was presided over by Vice President Mohammed Namadi Sambo , in the absence of President Goodluck Jonathan , who left as part of ECOWAS leaders to troubled Burkina Faso .";
//			JSONArray entities = NER.performAnnotation(text);
//			System.out.println(entities.toJSONString());
//		}
//		else{
//			if(args[1].equals("-ner")){
//				NER.insertNer(ACLEDColl, "NOTES");
//			}
//		}
//		database0.close();
	}
}

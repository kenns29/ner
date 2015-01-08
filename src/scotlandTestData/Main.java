package scotlandTestData;

import java.net.UnknownHostException;
import java.util.LinkedHashMap;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class Main {
	
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		Database database = new Database("fsdb1.dtn.asu.edu", 27017);
		//Database database = new Database("localhost", 27017);
		DB db = database.getDatabase("tweettracker");
		DBCollection sentenceColl = db.getCollection("tweets");
		
		System.out.println("Inserting Ner");
		insertNer(sentenceColl);
		System.out.println("Inserting Geocoding");
		insertLocation(sentenceColl);
		database.close();
	}
	
	public static void insertNer(DBCollection coll){
		BasicDBObject query = new BasicDBObject("ner", null);
		DBCursor cursor = coll.find(query);
		
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString("text");
				if(text != null && text.length() < 400){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					BasicDBList entities = NLP.annotateDBObject(text);
					
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
										new BasicDBObject("$set", new BasicDBObject("ner", entities)));
				}
			}
		}
		finally{
			cursor.close();
		}
	}
	
	public static void insertLocation(DBCollection coll){
		BasicDBObject query = new BasicDBObject("geocoding", null)
		.append("ner", new BasicDBObject("$ne", null));
		
		DBCursor cursor = coll.find(query);
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				BasicDBList ner = (BasicDBList) mongoObj.get("ner");
				BasicDBList locArray = new BasicDBList();
				BasicDBList outList = new BasicDBList();
				for(Object e: ner){
					BasicDBObject entity = (BasicDBObject)e;
					if(entity.getString("namedEntity").equals("LOCATION")){
						String inputLoc = entity.getString("mentionSpan");
						if(!(	inputLoc.equals("Africa")
								|| 	inputLoc.equals("Europe")
								|| 	inputLoc.equals("Asia")
								||	inputLoc.equals("North America")
								|| 	inputLoc.equals("South America")
								|| 	inputLoc.equals("Antarctica")	)){
							LinkedHashMap coord = GeoCoding.getCoord(inputLoc);
							String loc = GeoCoding.getCountry(coord);
							
							if(!locArray.contains(loc) && loc != null){
								outList.add(new BasicDBObject("name", loc)
								.append("coord", new BasicDBObject("lat", (double)coord.get("lat"))
													.append("lng", (double)coord.get("lng")))
								.append("type", "country"));
								locArray.add(loc);
							}
						}
					}
				}
				
				coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")), 
						new BasicDBObject("$set", new BasicDBObject("geocoding", outList)));
				
			}
		}
		finally{
			cursor.close();
		}
	}
}

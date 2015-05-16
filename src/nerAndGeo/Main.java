package nerAndGeo;

import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

import org.geonames.GeoNamesException;
import org.json.simple.JSONArray;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class Main {
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Database database = new Database("fsdb1.dtn.asu.edu", 27017);
		Database database1 = new Database("vaderserver0.cidse.dhcp.asu.edu", 27017);
		//Database database = new Database("localhost", 27017);
		DB dbTweettracker = database.getDatabase("tweettracker");
		DB dbRSS = database.getDatabase("foresight");
		DB dbACLED = database.getDatabase("conflicts");
		
		DB dbNigeriaTweets = database1.getDatabase("NigeriaTweets");
		
		DBCollection sentenceColl = dbRSS.getCollection("sentence");
		DBCollection tweetsColl = dbTweettracker.getCollection("tweets");
		DBCollection ACLEDColl = dbACLED.getCollection("ACLEDfull");
		
		DBCollection NigeriaColl = dbNigeriaTweets.getCollection("tweets");
		
		if(args[0].equals("-tweets")){
			if(args[1].equals("-ner")){
				if(args.length >= 3){
					Integer catID = Integer.parseInt(args[2]);
					BasicDBObject query = new BasicDBObject("cat", catID)
					.append("ner", null);
					
					insertNer(tweetsColl, "text", query);
				}
				else{
					insertNer(tweetsColl, "text");
				}
			}
			else if(args[1].equals("-location")){
				if(args.length >= 3){
					Integer catID = Integer.parseInt(args[2]);
					BasicDBObject query = new BasicDBObject("cat", catID)
					.append("ner", new BasicDBObject("$ne", null));
					
					insertLocation(tweetsColl, query);
				}
				else{
					insertLocation(tweetsColl);
				}
			}
			else{
				insertCoord(tweetsColl);
			}
		}
		else if(args[0].equals("-rss")){
			if(args[1].equals("-ner")){
				insertNer(sentenceColl, "sentence");
			}
			else if(args[1].equals("-location")){
				insertLocation(sentenceColl);
			}
			else if(args[1].equals("-geoname")){
				//insertGeonamesWithException(sentenceColl);
				insertGeoNames(sentenceColl);
			}
			else{
				insertCoord(sentenceColl);
			}
		}
		else if(args[0].equals("-NigeriaTweets")){
			if(args[1].equals("-ner")){
				insertNer(NigeriaColl, "text");
			}
			else if(args[1].equals("-location")){
				insertLocation(NigeriaColl);
			}
			else{
				
			}
		}
		else if(args[0].equals("-test")){
			String text = "Yesterday 's meeting was presided over by Vice President Mohammed Namadi Sambo , in the absence of President Goodluck Jonathan , who left as part of ECOWAS leaders to troubled Burkina Faso .";
			JSONArray entities = NLP.performAnnotation(text);
			System.out.println(entities.toJSONString());
		}
		else{
			if(args[1].equals("-ner")){
				insertNer(ACLEDColl, "NOTES");
			}
		}
		database.close();
	}
	
	public static void insertNer(DBCollection coll, String inputField){
		BasicDBObject query = new BasicDBObject("ner1", null);
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("There are total of " + cursor.count() + "items in the query");
		int count = 0;
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				if(text != null && text.length() < 1000){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					BasicDBList entities = NLP.annotateDBObject(text);
					
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
										new BasicDBObject("$set", new BasicDBObject("ner1", entities)));
					
					++count;
				}
				
				if(count % 100 == 0){
					System.out.println(count + " updated");
				}
			}
		}
		finally{
			cursor.close();
		}
	}
	
	public static void insertNer(DBCollection coll, String inputField, BasicDBObject query){
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("Finished Querying, there are total of " + cursor.count() + "items");
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				if(text != null && text.length() < 1000){
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
		
		insertLocation(coll, query);
	}
	
	public static void insertLocation(DBCollection coll, BasicDBObject query){
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("Finished query, there are total of " + cursor.count() + " items.");
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
	public static void insertCoord(DBCollection coll){
		BasicDBObject query = new BasicDBObject("geoCoord", null)
		.append("ner", new BasicDBObject("$ne", null));
		
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				BasicDBList ner = (BasicDBList) mongoObj.get("ner");
				BasicDBList outList = new BasicDBList();
				for(Object e : ner){
					BasicDBObject entity = (BasicDBObject) e;
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
							if(coord != null && loc != null){
								outList.add(new BasicDBObject("name", inputLoc)
									.append("country", loc)
									.append("coord", new BasicDBObject("lat", (double)coord.get("lat"))
													.append("lng", (double)coord.get("lng"))));
							}
						}
					}
				}
				coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
						new BasicDBObject("$set", new BasicDBObject("geoCoord", outList)));
			}
		}
		finally{
			cursor.close();
		}
	}
	
	public static void insertGeoNames(DBCollection coll, BasicDBObject query) throws Exception{
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		
		int count = 0;
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				BasicDBList ner = (BasicDBList) mongoObj.get("ner1");
				String text = mongoObj.getString("sentence");
				BasicDBList outList = new BasicDBList();
				for(Object e : ner){
					BasicDBObject entity = (BasicDBObject) e;
					String entType = entity.getString("namedEntity");
					String ent = entity.getString("mentionSpan");
					if(entType.equals("LOCATION")){
						BasicDBObject rObj = getGeonameMongoObj(ent);
					    if(rObj != null){
							outList.add(rObj);
						}
					}
//					else if(Pattern.matches("Burkina Faso", ent)){
//						BasicDBObject rObj = getGeonameMongoObj("Burkina Faso");
//					    if(rObj != null){
//							outList.add(rObj);
//						}
//					}
//					else if(ent.toLowerCase().equals("niger delta")){
//						BasicDBObject rObj = getGeonameMongoObj("Niger Delta");
//					    if(rObj != null){
//							outList.add(rObj);
//						}
//					}
				}
				
//				if(Pattern.matches(".*\\sCote d'Ivoire\\s.*", text)){
//					BasicDBObject rObj = getGeonameMongoObj("Cote d'Ivoire");	
//					if(rObj != null){
//						outList.add(rObj);
//					}
//				}
				coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")), 
						new BasicDBObject("$set", new BasicDBObject("geoname1", outList)));
				
				++count;
				if(count % 100 == 0){
					System.out.println(count + " updated");
				}
			}
		}
		finally{
			System.out.println("Finished Inserting Geonames");
			cursor.close();
		}
	}
	
	public static BasicDBObject getGeonameMongoObj(String name) throws Exception{
		BasicDBObject rObj = null;
		boolean reachLimit = false;
		do{
			try{
				System.out.println("Current Username = " + Geoname.accountName);
				rObj = Geoname.geocode(name);
				reachLimit = false;
			}
			catch(GeoNamesException exception){
				exception.printStackTrace();
				int code = exception.getExceptionCode();
				if(code == 19 || code == 10){
					Geoname.cycleAccountName();
					reachLimit = true;
				}
			}
		} while(reachLimit);
		return rObj;
	}
	public static void insertGeoNames(DBCollection coll) throws Exception{
		BasicDBObject query = new BasicDBObject("ner1", new BasicDBObject("$ne", null))
		.append("geoname1", null);
		insertGeoNames(coll, query);
	}
	
	public static void insertGeonamesWithException(DBCollection coll) throws Exception{
		
		//just for Niger Delta
//		BasicDBList orList = new BasicDBList();
//		orList.add(new BasicDBObject("geoname", null));
//		BasicDBList andList = new BasicDBList();
//		andList.add(new BasicDBObject("ner", new BasicDBObject("$ne", null)));
//		BasicDBList inList = new BasicDBList();
//		inList.add("Niger Delta");
//		andList.add(new BasicDBObject("ner.mentionSpan", new BasicDBObject("$in", inList)));
//		orList.add(new BasicDBObject("$and", andList));
//		BasicDBObject query = new BasicDBObject("ner", new BasicDBObject("$ne", null))
//		.append("$or", orList);
		
		BasicDBList andList = new BasicDBList();
		andList.add(new BasicDBObject("sentence", new BasicDBObject("$ne", null)));
		andList.add(new BasicDBObject("sentence", new BasicDBObject("$regex", "\\sCote d'Ivoire\\s")));
		insertGeoNames(coll, new BasicDBObject("$and", andList));
	}
	//http://www.geonames.org/export/webservice-exception.html
	
}

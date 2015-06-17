package nerAndGeo;

import java.util.List;
import java.util.logging.Logger;

import org.geonames.BoundingBox;
import org.geonames.GeoNamesException;
import org.geonames.InsufficientStyleException;
import org.geonames.Style;
import org.geonames.Toponym;
import org.geonames.ToponymSearchCriteria;
import org.geonames.ToponymSearchResult;
import org.geonames.WebService;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class Geoname {
	private static Logger LOGGER = Logger.getLogger(Geoname.class.getName());
	static{
		LOGGER.addHandler(LoggerAttr.fileHandler);
	}
	private static int accountNumber = 0;
	public static String accountName = getAccountName();
	public static String getAccountName(){
		if(accountNumber == 0){
			return "labuse";
		}
		else{
			return "labuse" + accountNumber;
		}
	}
	
	private static void cycleAccountNumber(){
		++accountNumber;
		if(accountNumber > 10){
			accountNumber = 0;
		}
	}
	
	public static void cycleAccountName(){
		cycleAccountNumber();
		accountName = getAccountName();
	}
	 public static BasicDBObject geocode(String name) throws Exception{
		 BasicDBObject rObj = null;
	 	 WebService.setUserName(accountName); // add your username here
	 	 //System.out.println("accountName = " + accountName);
		 //System.out.println("location = " + name);
		 ToponymSearchCriteria searchCriteria = new ToponymSearchCriteria();
		 searchCriteria.setStyle(Style.valueOf("FULL"));
		 ToponymSearchResult searchResult = null;
		 if(name.toLowerCase().equals("uk")){
			 searchCriteria.setQ(name);
			 searchCriteria.setCountryCode("UK");
			 
			 searchResult = WebService.search(searchCriteria);
			 if(searchResult.getTotalResultsCount() > 0){
				 List<Toponym> toponym = searchResult.getToponyms();
				 Toponym firstToponym = toponym.get(0);
				 rObj = getMongoObj(firstToponym);
			 }
		 }
		 else if(name.toLowerCase().equals("denmark")){
			 searchCriteria.setQ(name);
			 searchCriteria.setCountryCode("DK");
			 
			 searchResult = WebService.search(searchCriteria);
			 if(searchResult.getTotalResultsCount() > 0){
				 List<Toponym> toponym = searchResult.getToponyms();
				 Toponym firstToponym = toponym.get(0);
				 rObj = getMongoObj(firstToponym);
			 }
		 }
		 else{
			 //search for the full name first
			 searchCriteria.setName(name);
	
			 searchResult = WebService.search(searchCriteria);
			 
			 if(searchResult.getTotalResultsCount() > 0){
				 List<Toponym> toponyms = searchResult.getToponyms();
				 Toponym firstToponym = toponyms.get(0);
				 rObj = getMongoObj(firstToponym);
				 
			 }
			 else{
				 searchCriteria.setName("");
				 searchCriteria.setQ(name);
				 searchResult = WebService.search(searchCriteria);
				 if(searchResult.getTotalResultsCount() > 0){
					 List<Toponym> toponym = searchResult.getToponyms();
					 Toponym firstToponym = toponym.get(0);
					 rObj = getMongoObj(firstToponym);
				 }
				 
			 }
		 }
		 return rObj;
	 }
	 
	 public static BasicDBObject getMongoObj(Toponym toponym) throws InsufficientStyleException{
		 BasicDBObject rObj = new BasicDBObject();
		 String locName = toponym.getName();
		 String countryName = toponym.getCountryName();
		 String continentCode = toponym.getContinentCode();
		 String featureCode = toponym.getFeatureCode();
		 String featureClass = toponym.getFeatureClassName();
		 Long population = toponym.getPopulation();
		 BoundingBox boundingBox = toponym.getBoundingBox();
		 
		 double lat = toponym.getLatitude();
		 double lng = toponym.getLongitude();
		 
		 rObj.append("name", locName)
		 	.append("countryName", countryName);
		 if(continentCode != null){
			 rObj.append("continentCode", continentCode);
		 }
		 
		 if(population != null){
			 rObj.append("population", population);
		 }
		 
		 rObj.append("coord", new BasicDBObject("lat", lat)
				 				.append("lng", lng));
		 
		 if(boundingBox != null){
			 rObj.append("boundingBox", new BasicDBObject("north", boundingBox.getNorth())
					 						.append("west", boundingBox.getWest())
					 						.append("east", boundingBox.getEast())
					 						.append("south", boundingBox.getSouth()));
		 }
		 
		 
		 
		 if(featureCode != null){
			 rObj.append("featureCode", featureCode);
		 }
		 
		 if(featureClass != null){
			 rObj.append("featureClass", featureClass);
		 }
		 
		 return rObj;
	 }
	 
	 public static BasicDBObject getGeonameMongoObj(String name) throws Exception{
		BasicDBObject rObj = null;
		boolean reachLimit = false;
		do{
			try{
				rObj = Geoname.geocode(name);
				reachLimit = false;
			}
			catch(GeoNamesException exception){
				exception.printStackTrace();
				String preAccountName = Geoname.accountName;
				int code = exception.getExceptionCode();
				if(code == 19 || code == 10){
					synchronized(Geoname.class){
						if(preAccountName.equals(Geoname.accountName)){
							Geoname.cycleAccountName();
							LOGGER.config("Setting Current Geoname Account: " + Geoname.accountName);
						}
					}
					reachLimit = true;
				}
			}
		} while(reachLimit);
		return rObj;
	}
	 
	public static BasicDBList getGeonameList(BasicDBList ner) throws Exception{
		BasicDBList outList = new BasicDBList();
		for(Object e : ner){
			BasicDBObject entity = (BasicDBObject) e;
			String entType = entity.getString("namedEntity");
			String ent = entity.getString("mentionSpan");
			if(entType.equals("LOCATION")){
				BasicDBObject rObj = Geoname.getGeonameMongoObj(ent);
			    if(rObj != null){
					outList.add(rObj);
				}
			}
		}
		return outList;
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
						BasicDBObject rObj = Geoname.getGeonameMongoObj(ent);
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
}

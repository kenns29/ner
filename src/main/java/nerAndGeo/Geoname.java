package nerAndGeo;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.geonames.BoundingBox;
import org.geonames.GeoNamesException;
import org.geonames.InsufficientStyleException;
import org.geonames.Style;
import org.geonames.Toponym;
import org.geonames.ToponymSearchCriteria;
import org.geonames.ToponymSearchResult;
import org.geonames.WebService;

import util.ThreadStatus;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class Geoname {
	private static final int GEONAME_RETRY_LIMIT = 0;
	private static Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	
	private static Database cacheHost = null;
	private static DB cacheDB = null;
	private static DBCollection cacheColl = null;
	
	private static Database nullCacheHost = null;
	private static DB nullCacheDB = null;
	private static DBCollection nullCacheColl = null;
	
	private static AtomicInteger nameCount = new AtomicInteger(0);
	private static AtomicInteger cacheHitCount = new AtomicInteger(0);
	private static AtomicInteger geonameCount = new AtomicInteger(0);
	private static AtomicInteger perGeonameCount = new AtomicInteger(0);
	private static AtomicInteger perCacheHitCount = new AtomicInteger(0);
	
	private static AtomicInteger accountNumber = new AtomicInteger(0);
	static{
		try {
			cacheHost = new Database(Main.configPropertyValues.cacheHost, Main.configPropertyValues.cachePort);
			cacheDB = cacheHost.getDatabase(Main.configPropertyValues.cacheDatabase);
			cacheColl = cacheDB.getCollection(Main.configPropertyValues.cacheCollection);
			
			nullCacheHost = new Database(Main.configPropertyValues.nullCacheHost, Main.configPropertyValues.nullCachePort);
			nullCacheDB = nullCacheHost.getDatabase(Main.configPropertyValues.nullCacheDatabase);
			nullCacheColl = nullCacheDB.getCollection(Main.configPropertyValues.nullCacheCollection);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}

	public static String accountName = getAccountName();
	public static String getAccountName(){
		if(accountNumber.intValue() == 0){
			return "labuse";
		}
		else{
			return "labuse" + accountNumber;
		}
	}
	
	private static void cycleAccountNumber(){
		synchronized(Geoname.class){
			int n = accountNumber.incrementAndGet();
			if(n > 10){
				accountNumber.set(0);
			}
		}
	}
	
	public static void cycleAccountName(){
		cycleAccountNumber();
		accountName = getAccountName();
	}
	 public static BasicDBObject geocode(String name) throws Exception{
		 BasicDBObject rObj = null;
		 WebService.setConnectTimeOut(1);
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
				 rObj = getGeonameObj(firstToponym);
			 }
		 }
		 else if(name.toLowerCase().equals("denmark")){
			 searchCriteria.setQ(name);
			 searchCriteria.setCountryCode("DK");
			 
			 searchResult = WebService.search(searchCriteria);
			 if(searchResult.getTotalResultsCount() > 0){
				 List<Toponym> toponym = searchResult.getToponyms();
				 Toponym firstToponym = toponym.get(0);
				 rObj = getGeonameObj(firstToponym);
			 }
		 }
		 else{
			 //search for the full name first
			 searchCriteria.setName(name);
	
			 searchResult = WebService.search(searchCriteria);
			 
			 if(searchResult.getTotalResultsCount() > 0){
				 List<Toponym> toponyms = searchResult.getToponyms();
				 Toponym firstToponym = toponyms.get(0);
				 rObj = getGeonameObj(firstToponym);
				 
			 }
			 else{
				 searchCriteria.setName("");
				 searchCriteria.setQ(name);
				 searchResult = WebService.search(searchCriteria);
				 if(searchResult.getTotalResultsCount() > 0){
					 List<Toponym> toponym = searchResult.getToponyms();
					 Toponym firstToponym = toponym.get(0);
					 rObj = getGeonameObj(firstToponym);
				 }
				 
			 }
		 }
		 
		 
		 return rObj;
	 }
	 
	 public static BasicDBObject getGeonameObj(Toponym toponym) throws InsufficientStyleException{
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
		 
		 rObj.append("location", locName)
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
	 
	 
	 public static BasicDBObject getGeonameWithAccountRotate(String name, ThreadStatus threadStatus) throws Exception{
		BasicDBObject rObj = null;
		int unexpectedExceptionCount = 0;
		boolean retryFlag = false;
		do{
			String preAccountName = Geoname.accountName;
			try{
				
				rObj = Geoname.geocode(name);
				if(rObj != null){
					cacheGeonameObj(name, rObj);
				}
				retryFlag = false;
			}
			catch(GeoNamesException exception){
				int code = exception.getExceptionCode();
				//http://www.geonames.org/export/webservice-exception.html
				if(code == 19 || code == 10){
					synchronized(Geoname.class){
						if(preAccountName.equals(Geoname.accountName)){
							Geoname.cycleAccountName();
							LOGGER.info("Setting Current Geoname Account: " + Geoname.accountName);
						}
					}
					retryFlag = true;
				}
				else if(unexpectedExceptionCount < GEONAME_RETRY_LIMIT){
					++unexpectedExceptionCount;
					retryFlag = true;
					LOGGER.error("Geoname Error, Current Document encountered a Geoname Error, attempting retry." 
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to Unexpected Geoname Error", exception);
				}
				else{
					unexpectedExceptionCount = 0;
					retryFlag = false;
					HIGH_PRIORITY_LOGGER.error("Geoname Exception, Current Document is not fully processed by geoname."
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to Unexpected Geoname Error", exception);
					throw exception;
					
				}
			}
			catch(SocketTimeoutException exception){
				retryFlag = false;
				LOGGER.error("Java Error, Current Document encountered a Geoname Error, no retry." 
						+ "\nCurrent Thread Status: " + threadStatus.toString()
						+ "\nDue to SocketTimeoutException", exception);
				throw exception;
			}
			catch(Exception exception){
				if(unexpectedExceptionCount < GEONAME_RETRY_LIMIT){
					++unexpectedExceptionCount;
					retryFlag = true;
					LOGGER.error("Java Error, Current Document encountered a Geoname Error, attempting retry." 
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to Unexpected Geoname Error", exception);
				}
				else{
					unexpectedExceptionCount = 0;
					retryFlag = false;
					HIGH_PRIORITY_LOGGER.error("Java Exception, Current Document is not fully processed by geoname."
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to Unexpected Geoname Error", exception);
					throw exception;
				}
			}
		} while(retryFlag && unexpectedExceptionCount <= GEONAME_RETRY_LIMIT);
		return rObj;
	}
	public static BasicDBObject makeCacheObj(String name, BasicDBObject geonameObj){
		BasicDBObject rObj = new BasicDBObject("name", name);
		rObj.append("geoname", geonameObj);
		return rObj;
	}
	public static void cacheGeonameObj(String name, BasicDBObject geonameObj){
		BasicDBObject query = new BasicDBObject("name", name);
		if(cacheColl.findOne(query) == null){
			BasicDBObject cacheObj = makeCacheObj(name, geonameObj);
			cacheColl.update(query, new BasicDBObject("$set", cacheObj), true, false);
		}
	}
	public static BasicDBObject getGeonameObjFromCache(String name){
		BasicDBObject geonameObj = null;
		Object obj = cacheColl.findOne(new BasicDBObject("name", name));
		if(obj != null){
			geonameObj = (BasicDBObject) ((BasicDBObject) obj).get("geoname");
		}
		return geonameObj;
	}
	
	public static BasicDBObject getGeonameMongoObj(String name, ThreadStatus threadStatus) throws Exception{
		boolean cacheHit = false;
		boolean geonameHit = false;
		BasicDBObject geonameObj = null;
		if(Main.configPropertyValues.useGeonameCache){
			geonameObj = getGeonameObjFromCache(name);
		}
		
		if(geonameObj == null){
			geonameObj = getGeonameWithAccountRotate(name, threadStatus);
			if(geonameObj != null){
				geonameHit = true;
			}
		}
		else{
			cacheHit = true;
		}
		
		synchronized(Geoname.class){
			int nCount = Geoname.nameCount.incrementAndGet();
			int gCount = Geoname.geonameCount.intValue();
			int cCount = Geoname.cacheHitCount.intValue();
			int pgCount = Geoname.perGeonameCount.intValue();
			int pcCount = Geoname.perCacheHitCount.intValue();
			if(cacheHit){
				cCount = Geoname.cacheHitCount.incrementAndGet();
				pcCount = Geoname.perCacheHitCount.incrementAndGet();
			}
			else if(geonameHit){
				gCount = Geoname.geonameCount.incrementAndGet();
				pgCount = Geoname.perGeonameCount.incrementAndGet();
			}
			if(nCount % 5000 == 0){
				DecimalFormat df = new DecimalFormat("#.00");
				LOGGER.info("Overall, " + nCount + " names has been encountered, " + gCount + " are geocoded with geoname, " + cCount + " are found from geoname cache.\n"
						+ "\nAmong the last 5000 names, " + pgCount + " are geocoded with geoname, " + pcCount + " are found from the geoname cache."
						+ "\nThe cache hit rate is " + df.format((double)pcCount/5000)
						+ "\nThe overall cache hit rate is " + df.format((double)cCount/ nCount));
				Geoname.perGeonameCount.set(0);
				Geoname.perCacheHitCount.set(0);
			}
		}
		return geonameObj;
	}
	public static BasicDBList makeGeonameList(BasicDBList ner, ThreadStatus threadStatus) throws Exception{
		return makeGeonameList(ner, null, threadStatus);
	}
	public static BasicDBList makeGeonameList(BasicDBList ner, String flag, ThreadStatus threadStatus) throws Exception{
		BasicDBList outList = new BasicDBList();
		for(Object e : ner){
			BasicDBObject entity = (BasicDBObject) e;
			String entType = entity.getString("entityType");
			String ent = entity.getString("entity");
			if(entType.equals("LOCATION")){
				BasicDBObject rObj = Geoname.getGeonameMongoObj(ent, threadStatus);
				
			    if(rObj != null){
					if(flag != null){
						rObj.put("from", flag);
					}
			    	outList.add(rObj);
				}
			    
			}
		}
		return outList;
	}
	
	public static BasicDBList makeNerGeonameList(BasicDBList ner, ThreadStatus threadStatus) throws Exception{
		return makeNerGeonameList(ner, null, threadStatus);
	}
	
	public static BasicDBList makeNerGeonameList(BasicDBList ner, String flag, ThreadStatus threadStatus) throws Exception{
		BasicDBList outList = new BasicDBList();
		for(Object e : ner){
			BasicDBObject entity = (BasicDBObject) e;
			String entType = entity.getString("entityType");
			String ent = entity.getString("entity");
			
			if(entType.equals("LOCATION")){
				BasicDBObject geonameObj = Geoname.getGeonameMongoObj(ent, threadStatus);
				if(geonameObj != null){
					entity.put("geoname", geonameObj);
				}
			}
			
			if(flag != null){
				entity.put("from", flag);
			}
			outList.add(entity);
		}
		return outList;
	}
	///////////////////////////////
	//Single Thread operations/////
	///////////////////////////////
	public static void insertGeoNames(DBCollection coll, BasicDBObject query) throws Exception{
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		
		int count = 0;
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				BasicDBList ner = (BasicDBList) mongoObj.get("ner1");
//				String text = mongoObj.getString("sentence");
				BasicDBList outList = new BasicDBList();
				for(Object e : ner){
					BasicDBObject entity = (BasicDBObject) e;
					String entType = entity.getString("entityType");
					String ent = entity.getString("entity");
					if(entType.equals("LOCATION")){
						BasicDBObject rObj = Geoname.getGeonameMongoObj(ent, new ThreadStatus(0));
					    if(rObj != null){
							outList.add(rObj);
						}
					}
//					else if(Pattern.matches("Burkina Faso", ent)){
//						BasicDBObject rObj = getGeonameWithAccountRotate("Burkina Faso");
//					    if(rObj != null){
//							outList.add(rObj);
//						}
//					}
//					else if(ent.toLowerCase().equals("niger delta")){
//						BasicDBObject rObj = getGeonameWithAccountRotate("Niger Delta");
//					    if(rObj != null){
//							outList.add(rObj);
//						}
//					}
				}
				
//				if(Pattern.matches(".*\\sCote d'Ivoire\\s.*", text)){
//					BasicDBObject rObj = getGeonameWithAccountRotate("Cote d'Ivoire");	
//					if(rObj != null){
//						outList.add(rObj);
//					}
//				}
				coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")), 
						new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.geonameOutputField, outList)));
				
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
		BasicDBObject query = new BasicDBObject("ner", new BasicDBObject("$ne", null))
		.append("geoname", null);
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
//		andList.add(new BasicDBObject("ner.entity", new BasicDBObject("$in", inList)));
//		orList.add(new BasicDBObject("$and", andList));
//		BasicDBObject query = new BasicDBObject("ner", new BasicDBObject("$ne", null))
//		.append("$or", orList);
		
		BasicDBList andList = new BasicDBList();
		andList.add(new BasicDBObject("sentence", new BasicDBObject("$ne", null)));
		andList.add(new BasicDBObject("sentence", new BasicDBObject("$regex", "\\sCote d'Ivoire\\s")));
		insertGeoNames(coll, new BasicDBObject("$and", andList));
	}
}

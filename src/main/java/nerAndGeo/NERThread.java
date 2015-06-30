package nerAndGeo;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import util.TimeRange;
import util.TimeUtilities;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class NERThread implements Runnable{
	private static final Logger LOGGER = Logger.getLogger(NERThread.class.getName());
	static{
		//LOGGER.addHandler(LoggerAttr.consoleHandler);
		LOGGER.addHandler(LoggerAttr.fileHandler);
	}
	private DBCollection coll = null;
	private String inputField = null;
	
	protected final BlockingQueue<TimeRange> queue;
	
	/////////////////////////////////
	/////////Private Methods/////////
	/////////////////////////////////	
	private BasicDBObject constructQuery(TimeRange timeRange){
		long startTime = timeRange.startTime;
		long endTime = timeRange.endTime;
		
		BasicDBObject query = null;
		//do not use the cat id
		if(Main.configPropertyValues.catID < 0){
			switch(Main.configPropertyValues.parallelFlag){
			case 1:
				query = new BasicDBObject("timestamp", 
						new BasicDBObject("$gte", startTime)
						.append("$lt", endTime));
				break;
			case 0:
			default:
				query = new BasicDBObject("_id", new BasicDBObject("$gte", TimeUtilities.getObjectIdFromTimestamp(startTime))
				.append("$lt", TimeUtilities.getObjectIdFromTimestamp(endTime)));

			}
			
		}
		//use the cat id
		else{
			switch(Main.configPropertyValues.parallelFlag){
			case 1:
				query = new BasicDBObject("cat", Main.configPropertyValues.catID)
				.append("timestamp", 
					new BasicDBObject("$gte", startTime)
					.append("$lt", endTime));
				break;
			case 0:
			default:
				query = new BasicDBObject("cat", Main.configPropertyValues.catID)
				.append("_id", new BasicDBObject("$gte", TimeUtilities.getObjectIdFromTimestamp(startTime))
								.append("$lt", TimeUtilities.getObjectIdFromTimestamp(endTime)));
			}
		}
		
		return query;
	}

	public NERThread(DBCollection coll, String inputField, BlockingQueue<TimeRange> queue){
		this.coll = coll;
		this.inputField = inputField;
		this.queue = queue;
		
	}
	
	
	
	@Override
	public void run() {
		TimeRange timeRange = new TimeRange(0, 0);
		while(true){
			try {
				timeRange = queue.take();
				LOGGER.info("Started new Thread for " + timeRange.toString());
				Properties NLPprops = new Properties();
				NLPprops.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
				StanfordCoreNLP pipeline = new StanfordCoreNLP(NLPprops);
				long start = System.currentTimeMillis();
				try {
					insertNerGeo(timeRange, pipeline);
				} catch (Exception e) {
					LOGGER.warning("ner parsing error");
					e.printStackTrace();
				}
				long time = System.currentTimeMillis() - start;
				LOGGER.info("Finished Thread for " + timeRange.toString() +". Elapsed Time = " + time);
			} catch (InterruptedException e1) {
				LOGGER.severe("TAKING " + timeRange.toString() + " is INTERRUPTED");
				e1.printStackTrace();
			}
		}
	}
	
	public void insertNerGeo(TimeRange timeRange, StanfordCoreNLP pipeline) throws Exception{
		BasicDBObject query = constructQuery(timeRange);
		DBCursor cursor = null;
		if(Main.configPropertyValues.splitOption == 0){
			cursor = coll.find(query).sort(new BasicDBObject("_id", 1));
		}
		else{
			cursor = coll.find(query).sort(new BasicDBObject("_id", 1)).limit(Main.configPropertyValues.numDocsInThread);
		}
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		LOGGER.info("Querying for "+ timeRange.toString() + ", there are total of " + cursor.count() + " items");
//			+ "\nequivalent to from ObjectId " + TimeUtilities.getObjectIdFromTimestamp(startTime) + " to " + TimeUtilities.getObjectIdFromTimestamp(endTime)
//			+ "\nQuery = " + query.toString());
		if(Main.configPropertyValues.geoname){
			LOGGER.info("inserting entities along with geonames");
		}
		else{
			LOGGER.info("inserting entities");
		}
		try{
			while(cursor.hasNext()){
				++Main.documentCount;
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				String userText = null;
				
				BasicDBList userEntities = null;
				BasicDBList textEntities = null;
				
//				System.out.println("tweetId = " + mongoObj.getLong("id"));
//				System.out.println("text = " + text);
				if(Main.configPropertyValues.userNer){
					BasicDBObject userObj = (BasicDBObject) mongoObj.get("user");
					userText = userObj.getString("location");
					
					if(userText != null){
//						System.out.println("userText = " + userText);
						userEntities = NER.annotateDBObject(userText, pipeline, timeRange);
						userEntities = NER.insertFromFlag(userEntities, "user.location");
					}
				}
				
				if(text != null && text.length() < 1000){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					textEntities = NER.annotateDBObject(text, pipeline, timeRange);
					textEntities = NER.insertFromFlag(textEntities, Main.configPropertyValues.nerInputField);
				}
				
				BasicDBList entities = new BasicDBList();
				if(textEntities != null){
					if(textEntities.size() > 0){
						++NER.textEntitiesDocCount;
					}
					entities.addAll(textEntities);
				}
				if(userEntities != null){
					if(userEntities.size() > 0){
						++NER.userEntitiesDocCount;
					}
					entities.addAll(userEntities);
				}
				
				BasicDBObject coordinates = (BasicDBObject) mongoObj.get("coordinates");
				BasicDBObject place = (BasicDBObject) mongoObj.get("place");
				BasicDBObject location = (BasicDBObject) mongoObj.get("location");
				
				GeojsonList geojsonList = new GeojsonList();
				
				geojsonList.addFromMongoCoord(coordinates, place, location);
				if(!Main.configPropertyValues.geoname){
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
							new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)));
				}
				else if(Main.configPropertyValues.outputOption == 0){
					BasicDBList nerGeonameList = Geoname.makeNerGeonameList(entities);
					geojsonList.addFromNerGeonameList(nerGeonameList);
//					System.out.println(nerGeonameList.toString());
					if(geojsonList.isEmpty()){
						coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
								new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, nerGeonameList)));
					}
					else{
						coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
								new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, nerGeonameList)
																.append(Main.configPropertyValues.geojsonListOutputField, geojsonList.geometryCollection)));
					}
					
				}
				else if(Main.configPropertyValues.outputOption == 1){
					BasicDBList geonameList = Geoname.makeGeonameList(entities);
					geojsonList.addFromGeonameList(geonameList);
					if(geojsonList.isEmpty()){
						coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
								new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)
															.append(Main.configPropertyValues.geonameOutputField, geonameList)));
					}
					else{
						coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
								new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)
															.append(Main.configPropertyValues.geonameOutputField, geonameList)
															.append(Main.configPropertyValues.geojsonListOutputField, geojsonList.geometryCollection)));
					}
					
				}
				
				
				++NERTaskManager.count;
				long time = System.currentTimeMillis();
				if(time - NERTaskManager.preTime >= 60000){
					LOGGER.info("From " + NERTaskManager.preTime + " to " + time + ", " + NERTaskManager.count + " are processed. The time range is " + (time - NERTaskManager.preTime));
					NERTaskManager.preTime = time;
					NERTaskManager.count = 0;
				}
				
				if(Main.documentCount % 100 == 0){
					LOGGER.info(Main.documentCount + " documents has been processed. " + NER.textEntitiesDocCount + " documents has entities from text. " + NER.userEntitiesDocCount + " documents has entities from user profile location.");
				}
			}
		}
		finally{
			cursor.close();
		}
	}	
}

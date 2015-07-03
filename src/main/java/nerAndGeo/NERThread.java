package nerAndGeo;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
//import java.util.logging.Logger;

import util.ThreadStatus;
import util.TimeRange;
import util.TimeUtilities;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.log4j.*;

public class NERThread implements Runnable{
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static final Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final int RETRY_LIMIT = 2;
//	static{
//		LOGGER.addHandler(LoggerAttr.fileHandler);
//	}
	
	private DBCollection coll = null;
	private String inputField = null;
	private int unexpectedExceptionCount = 0;
	
	public ThreadStatus threadStatus = null;
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

	public NERThread(DBCollection coll, String inputField, BlockingQueue<TimeRange> queue, ThreadStatus threadStatus){
		this.coll = coll;
		this.inputField = inputField;
		this.queue = queue;
		this.threadStatus = threadStatus;
	}
	
	
	
	@Override
	public void run() {
		this.threadStatus.systemThreadId = Thread.currentThread().getId();
		TimeRange timeRange = null;
		while(true){
			try {
				this.threadStatus.isBlocked = true;
				timeRange = queue.take();
				this.threadStatus.timeRange = timeRange;
				this.threadStatus.isBlocked = false;
				
			} catch (InterruptedException e1) {
				LOGGER.info("TAKING " + timeRange.toString() + " is INTERRUPTED"
						+ "\nStack trace: " + e1.getStackTrace());
				continue;
			} 
			
			boolean retryFlag = false;
			do{
				try{
					LOGGER.info("Started new Thread for " + timeRange.toString());
					Properties NLPprops = new Properties();
					NLPprops.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
					StanfordCoreNLP pipeline = new StanfordCoreNLP(NLPprops);
					long start = System.currentTimeMillis();
					try {
						insertNerGeo(timeRange, pipeline);
					} catch (Exception e) {
						LOGGER.info("ner parsing error");
						e.printStackTrace();
					}
					long time = System.currentTimeMillis() - start;
					LOGGER.info("Finished Thread for " + timeRange.toString() +". Elapsed Time = " + time);
				}
				catch(Exception e){
					++this.unexpectedExceptionCount;
					LOGGER.error("Unexpected Exception on " + timeRange.toString() + " occured.", e);
					
					if(this.unexpectedExceptionCount >= RETRY_LIMIT){
						retryFlag = false;
						String msg = "Time Range " + timeRange.toString() + " has not been fully processed."
									+ "\nPossible document that causes the error is " + this.threadStatus.currentObjectId + "."
									+ "\nCurrent Thread Status is " + this.threadStatus.toString() + ".";
						
						HIGH_PRIORITY_LOGGER.fatal(msg, e);
						this.unexpectedExceptionCount = 0;
					}
					else{
						retryFlag = true;
					}
				}
			}
			while(retryFlag);
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
		int numDocs = cursor.count();
		this.threadStatus.numDocs = numDocs;
		LOGGER.info("Querying for "+ timeRange.toString() + ", there are total of " + numDocs + " items");	
		try{
			while(cursor.hasNext()){
				int documentCount = Main.documentCount.incrementAndGet();
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				this.threadStatus.currentObjectId = mongoObj.getObjectId("_id");
				this.threadStatus.currentInsertionTime = TimeUtilities.getTimestampFromObjectId(this.threadStatus.currentObjectId);
				this.threadStatus.currentTweetId = mongoObj.getLong("id");
				
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
				int textEntitiesDocCount = Main.textEntitiesDocCount.intValue();
				int userEntitiesDocCount = Main.userEntitiesDocCount.intValue();
				if(textEntities != null){
					if(textEntities.size() > 0){
						textEntitiesDocCount =  Main.textEntitiesDocCount.incrementAndGet();
					}
					entities.addAll(textEntities);
				}
				if(userEntities != null){
					if(userEntities.size() > 0){
						userEntitiesDocCount = Main.userEntitiesDocCount.incrementAndGet();
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
				
				
				Main.timelyDocCount.incrementAndGet();
				if(documentCount % 100 == 0){
					LOGGER.info(documentCount + " documents has been processed. " + textEntitiesDocCount + " documents has entities from text. " + userEntitiesDocCount + " documents has entities from user profile location.");
				}
			}
		}
		finally{
			cursor.close();
		}
	}	
}

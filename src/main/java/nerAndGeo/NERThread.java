package nerAndGeo;

import java.io.FileNotFoundException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import util.CollUtilities;
import util.ErrorStatus;
//import java.util.logging.Logger;
import util.ErrorType;
import util.RetryCacheCollUtilities;
import util.TaskType;
import util.ThreadStatus;
import util.TimeRange;
import util.TimeUtilities;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import timer.DocumentProcessTimeHandler;
import timer.timeObj.DocumentProcessTime;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.*;

public class NERThread implements Runnable{
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static final Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final int RETRY_LIMIT = 0; // do not want to retry anymore
//	static{
//		LOGGER.addHandler(LoggerAttr.fileHandler);
//	}
	private long threadFinishedTime = 0;
		
	private DBCollection coll = null;
	private String inputField = null;
	private int unexpectedExceptionCount = 0;
	
	public ThreadStatus threadStatus = null;
	protected final BlockingQueue<TimeRange> queue;
	
	public AtomicInteger docCount = new AtomicInteger(0);
	/////////////////////////////////
	/////////Private Methods/////////
	/////////////////////////////////	
	private BasicDBObject constructQuery(TimeRange timeRange){
		long startTime = timeRange.startTime;
		long endTime = timeRange.endTime;
		
		BasicDBObject query = null;
		//do not use the cat id
		if(Main.configPropertyValues.catID < 0){
			switch(Main.configPropertyValues.useInsertionOrCreationTime){
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
			switch(Main.configPropertyValues.useInsertionOrCreationTime){
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
		long threadStartTime = System.currentTimeMillis();
		synchronized(NER.class){
			this.threadStatus.systemThreadId = Thread.currentThread().getId();
		}
		TimeRange timeRange = null;
		while(true){
			try {
				synchronized(NER.class){
					this.threadStatus.isBlocked = true;
				}
				timeRange = queue.take();
				synchronized(NER.class){
					this.threadStatus.timeRange = timeRange;
					this.threadStatus.isBlocked = false;
				}
				
			} catch (InterruptedException e1) {
				LOGGER.info("TAKING " + timeRange.toString() + " is INTERRUPTED"
						+ "\nStack trace: " + e1.getStackTrace());
				continue;
			} 
			
			boolean retryFlag = false;
			do{
				try{
					LOGGER.info("Started new Thread for " + timeRange.toString() + " with Object Id Range " + timeRange.toObjectIdString());
					Properties NLPprops = new Properties();
					NLPprops.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
					StanfordCoreNLP pipeline = new StanfordCoreNLP(NLPprops);
					long start = System.currentTimeMillis();
					insertNerGeo(timeRange, pipeline);
					long time = System.currentTimeMillis() - start;
					LOGGER.info("Finished Thread for " + timeRange.toString() + " with Object Id Range " + timeRange.toObjectIdString() +". Elapsed Time = " + time);
					retryFlag = false;
				}
				catch(Exception e){
					++this.unexpectedExceptionCount;
					LOGGER.error("Unexpected Exception on " + timeRange.toString() + " with Object Id Range " + timeRange.toObjectIdString() + " occured.", e);
					
					if(this.unexpectedExceptionCount > RETRY_LIMIT){
						retryFlag = false;
						String msg = "Time Range " + timeRange.toString() + " with Object Id Range " + timeRange.toObjectIdString() + " was possibly not fully processed."
									+ "\nPossible document that causes the error is " + this.threadStatus.currentObjectId + "."
									+ "\nCurrent Thread Status is " + this.threadStatus.toString() + ".";
						
						HIGH_PRIORITY_LOGGER.fatal(msg, e);
						this.unexpectedExceptionCount = 0;
						if(this.threadStatus.timeRange.taskType.getType() == TaskType.RETRY_TASK){
							Main.retryCacheAvailable = true;
						}
					}
					else{
						retryFlag = true;
						LOGGER.error("Retry " + timeRange.toString() + " with Object Id Range " + timeRange.toObjectIdString());
					}
				}
			}
			while(retryFlag);
			
			long threadEndTime = System.currentTimeMillis();
			synchronized(Main.lockObjectThreadFinishCount){
				if(Main.totalThreadFinishedTime >= Long.MAX_VALUE - (threadEndTime - threadStartTime) - 5000){
					Main.totalThreadFinishedTime = 0;
					Main.threadFinishCount.set(0);
				}
				Main.totalThreadFinishedTime += (threadEndTime - threadStartTime);
				Main.threadFinishCount.incrementAndGet();
			}
			
			if(this.threadStatus.timeRange.taskType.getType() == TaskType.RETRY_TASK){
				Main.retryCacheAvailable = true;
			}
		}
	}
	
	public void insertNerGeo(TimeRange timeRange, StanfordCoreNLP pipeline) throws Exception{
		BasicDBObject query = constructQuery(timeRange);
		if(Main.configPropertyValues.splitOption == 0){
			DBCursor cursor = coll.find(query).sort(new BasicDBObject("_id", 1));
			cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
			insertNerGeoFromCursor(cursor, timeRange, pipeline);
		}
		else{
			insertNerGeoFromArray(timeRange, pipeline);
		}
	}	
	
	public void insertNerGeoFromCursor(DBCursor cursor, TimeRange timeRange, StanfordCoreNLP pipeline) throws Exception{
		int numDocs = cursor.count();
		this.threadStatus.numDocs = numDocs;
		LOGGER.info("Querying for "+ timeRange.toString() + ", there are total of " + numDocs + " items");	
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				insertOneNerGeo(mongoObj, timeRange, pipeline);
			}
		}
		finally{
			cursor.close();
		}
	}

	public boolean insertNerGeoFromArray(TimeRange timeRange, StanfordCoreNLP pipeline) throws Exception{
		this.threadStatus.numDocs = timeRange.mongoObjList.size();
		//Normal Task
		if(timeRange.taskType.getType() == TaskType.NORMAL_TASK){
			for(DBObject obj : timeRange.mongoObjList){
				BasicDBObject mongoObj = (BasicDBObject) obj;
				try{
					insertOneNerGeo(mongoObj, timeRange, pipeline);
				}
				catch(SocketTimeoutException e){
					RetryCacheCollUtilities.insert(Main.retryCacheColl, mongoObj, new ErrorStatus(new ErrorType(ErrorType.SOCKET_TIME_OUT), 1, ExceptionUtils.getStackTrace(e)));
					HIGH_PRIORITY_LOGGER.error("Java Error, Encounted a Socket time out error while processing document " + this.threadStatus.currentObjectId +". Inserting the document to retry cache" 
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to SocketTimeoutException. ", e);
					
					synchronized(Main.lockObjectGeonameServiceChecker){
						if(Main.geonameServiceAvailable){
							Main.geonameServiceAvailable = false;
							Main.geonameServiceCheckerThread = new Thread(new GeonameServiceChecker());
							Main.geonameServiceCheckerThread.start();
						}
					}
					
				}
				catch(ConnectException e){
					RetryCacheCollUtilities.insert(Main.retryCacheColl, mongoObj, new ErrorStatus(new ErrorType(ErrorType.CONNECT_EXCEPTION), 1, ExceptionUtils.getStackTrace(e)));
					HIGH_PRIORITY_LOGGER.error("Java Error, Encounted a Socket time out error while processing document " + this.threadStatus.currentObjectId +". Inserting the document to retry cache" 
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to ConnectException. ", e);
					
					synchronized(Main.lockObjectGeonameServiceChecker){
						if(Main.geonameServiceAvailable){
							Main.geonameServiceAvailable = false;
							Main.geonameServiceCheckerThread = new Thread(new GeonameServiceChecker());
							Main.geonameServiceCheckerThread.start();
						}
					}
				}
				catch(FileNotFoundException e){
					RetryCacheCollUtilities.insert(Main.retryCacheColl, mongoObj, new ErrorStatus(new ErrorType(ErrorType.FILE_NOT_FOUND), 1, ExceptionUtils.getStackTrace(e)));
					HIGH_PRIORITY_LOGGER.error("Encounted an error while processing document " + this.threadStatus.currentObjectId 
											+ "\nCurrent Thread Status: " + this.threadStatus.toString()
											+ "\nDue to FileNotFound Exception. ", e);
					
					synchronized(Main.lockObjectGeonameServiceChecker){
						if(Main.geonameServiceAvailable){
							Main.geonameServiceAvailable = false;
							Main.geonameServiceCheckerThread = new Thread(new GeonameServiceChecker());
							Main.geonameServiceCheckerThread.start();
						}
					}
				}
				catch(Exception e){
					RetryCacheCollUtilities.insert(Main.errorCacheColl, mongoObj, new ErrorStatus(new ErrorType(ErrorType.OTHER), 1, ExceptionUtils.getStackTrace(e)));
					HIGH_PRIORITY_LOGGER.error("Encounted an error while processing document " + this.threadStatus.currentObjectId 
											+ "\nCurrent Thread Status: " + this.threadStatus.toString()
											+ "\nDue to Unexpected Exception. ", e);
					
				}
				
				docCount.incrementAndGet();
			}
		}
		//Retry Task
		else{
			for(DBObject obj : timeRange.mongoObjList){
				BasicDBObject mongoObj = (BasicDBObject) obj;
				try{
					insertOneNerGeo(mongoObj, timeRange, pipeline);
					RetryCacheCollUtilities.remove(Main.retryCacheColl, mongoObj);
				}
				catch(SocketTimeoutException e){
					ErrorStatus errorStatus = RetryCacheCollUtilities.updateErrorTypeOrCount(Main.retryCacheColl, mongoObj, ErrorType.SOCKET_TIME_OUT, e);
					HIGH_PRIORITY_LOGGER.error("Java Error, Encounted a SocketTimeoutException while processing document " + this.threadStatus.currentObjectId +" in the retry cache."
							+ "\nThere have been total of " + errorStatus.getErrorCount() + " such errors."
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to SocketTimeoutException. ", e);
					
					synchronized(Main.lockObjectGeonameServiceChecker){
						if(Main.geonameServiceAvailable){
							Main.geonameServiceAvailable = false;
							Main.geonameServiceCheckerThread = new Thread(new GeonameServiceChecker());
							Main.geonameServiceCheckerThread.start();
						}
					}
					
					if(timeRange.taskType.getType() == TaskType.RETRY_TASK){
						HIGH_PRIORITY_LOGGER.error("exit the Retry Task");
						return false;
					}
				}
				catch(ConnectException e){
					ErrorStatus errorStatus = RetryCacheCollUtilities.updateErrorTypeOrCount(Main.retryCacheColl, mongoObj, ErrorType.CONNECT_EXCEPTION, e);
					HIGH_PRIORITY_LOGGER.error("Java Error, Encounted a ConnectException while processing document " + this.threadStatus.currentObjectId +" in the retry cache."
							+ "\nThere have been total of " + errorStatus.getErrorCount() + " such errors."
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to ConnectException. ", e);
					
					synchronized(Main.lockObjectGeonameServiceChecker){
						if(Main.geonameServiceAvailable){
							Main.geonameServiceAvailable = false;
							Main.geonameServiceCheckerThread = new Thread(new GeonameServiceChecker());
							Main.geonameServiceCheckerThread.start();
						}
					}
					
					if(timeRange.taskType.getType() == TaskType.RETRY_TASK){
						HIGH_PRIORITY_LOGGER.error("exit the Retry Task");
						return false;
					}
				}
				catch(FileNotFoundException e){
					ErrorStatus errorStatus = RetryCacheCollUtilities.updateErrorTypeOrCount(Main.retryCacheColl, mongoObj, ErrorType.FILE_NOT_FOUND, e);
					HIGH_PRIORITY_LOGGER.error("Java Error, Encounted an error while processing document " + this.threadStatus.currentObjectId +" in the retry cache." 
							+ "\nThere have been total of " + errorStatus.getErrorCount() + " such errors."
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to FileNotFoundException Exception. ", e);
					
					synchronized(Main.lockObjectGeonameServiceChecker){
						if(Main.geonameServiceAvailable){
							Main.geonameServiceAvailable = false;
							Main.geonameServiceCheckerThread = new Thread(new GeonameServiceChecker());
							Main.geonameServiceCheckerThread.start();
						}
					}
					
					if(timeRange.taskType.getType() == TaskType.RETRY_TASK){
						HIGH_PRIORITY_LOGGER.error("exit the Retry Task");
						return false;
					}
				}
				catch(Exception e){	
					ErrorStatus errorStatus = RetryCacheCollUtilities.updateErrorTypeOrCount(Main.errorCacheColl, mongoObj, ErrorType.OTHER, e);
					if(timeRange.taskType.getType() == TaskType.RETRY_TASK){
						RetryCacheCollUtilities.remove(Main.retryCacheColl, mongoObj);
					}
					HIGH_PRIORITY_LOGGER.error("Java Error, Encounted an error while processing document " + this.threadStatus.currentObjectId +" in the retry cache." 
							+ "\nThere have been total of " + errorStatus.getErrorCount() + " such errors."
							+ "\nCurrent Thread Status: " + threadStatus.toString()
							+ "\nDue to Unexpected Exception. ", e);
				}
				
			}
		}
		return true;
	}
	
	public void insertOneNerGeo(BasicDBObject mongoObj, TimeRange timeRange, StanfordCoreNLP pipeline) throws Exception{
		long docStartTime = System.currentTimeMillis();
		synchronized(NER.class){
			this.threadStatus.currentMongoObj = mongoObj;
			this.threadStatus.currentObjectId = mongoObj.getObjectId("_id");
			this.threadStatus.currentInsertionTime = TimeUtilities.getTimestampFromObjectId(this.threadStatus.currentObjectId);
			if (mongoObj.containsField("id")) {
				this.threadStatus.currentTweetId = mongoObj.getLong("id");
			}
		}
		CollUtilities.unsetLocationCollection(coll, mongoObj);
		int documentCount = Main.documentCount.incrementAndGet();
		DocumentProcessTime documentProcessTime = new DocumentProcessTime();
		String text = mongoObj.getString(inputField);
		String userText = null;
		
		BasicDBList userEntities = null;
		BasicDBList textEntities = null;
		
		if(Main.configPropertyValues.userNer){
			Object tUserObj = mongoObj.get("user");
			BasicDBObject userObj = null;
			if(tUserObj != null){
				userObj = (BasicDBObject) tUserObj;
			}
			if(userObj != null){
				userText = userObj.getString("location");
			}
			if(userText != null){
				long userNerStartTime = System.currentTimeMillis();
				userEntities = NER.annotateDBObject(userText, pipeline, timeRange);
				userEntities = NER.insertFromFlag(userEntities, "user.location");
				long userNerEndTime = System.currentTimeMillis();
				documentProcessTime.setUserNerTime(userNerEndTime - userNerStartTime);
			}
		}
		
		if(text != null && text.length() < 1000){
			text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
			long nerStartTime = System.currentTimeMillis();
			textEntities = NER.annotateDBObject(text, pipeline, timeRange);
			textEntities = NER.insertFromFlag(textEntities, Main.configPropertyValues.nerInputField);
			long nerEndTime = System.currentTimeMillis();
			documentProcessTime.setNerTime(nerEndTime - nerStartTime);
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
			long updateStartTime = System.currentTimeMillis();
			Main.outputColl.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
					new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)));
			long updateEndTime = System.currentTimeMillis();
			documentProcessTime.setMongoUpdateTime(updateEndTime - updateStartTime);
		}
		else if(Main.configPropertyValues.outputOption == 0){
			long nerGeonameStartTime = System.currentTimeMillis();
			BasicDBList nerGeonameList = Geoname.makeNerGeonameList(entities, this.threadStatus);
			long nerGeonameEndTime = System.currentTimeMillis();
			documentProcessTime.setNerGeonameTime(nerGeonameEndTime - nerGeonameStartTime);
			
			long geojsonStartTime = System.currentTimeMillis();
			geojsonList.addFromNerGeonameList(nerGeonameList);
			long geojsonEndTime = System.currentTimeMillis();
			documentProcessTime.setGeojsonTime(geojsonEndTime - geojsonStartTime);

			if(geojsonList.isEmpty()){
				long updateStartTime = System.currentTimeMillis();
				Main.outputColl.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
						new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, nerGeonameList)));
				long updateEndTime = System.currentTimeMillis();
				
				documentProcessTime.setMongoUpdateTime(updateEndTime - updateStartTime);
				
			}
			else{
				long updateStartTime = System.currentTimeMillis();
				Main.outputColl.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
						new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, nerGeonameList)
														.append(Main.configPropertyValues.geojsonListOutputField, geojsonList.geometryCollection)));
				long updateEndTime = System.currentTimeMillis();
				documentProcessTime.setMongoUpdateTime(updateEndTime - updateStartTime);
			}
			
		}
		else if(Main.configPropertyValues.outputOption == 1){
			long geonameStartTime = System.currentTimeMillis();
			BasicDBList geonameList = Geoname.makeGeonameList(entities, this.threadStatus);
			long geonameEndTime = System.currentTimeMillis();
			documentProcessTime.setNerGeonameTime(geonameEndTime - geonameStartTime);
			
			long geojsonStartTime = System.currentTimeMillis();
			geojsonList.addFromGeonameList(geonameList);
			long geojsonEndTime = System.currentTimeMillis();
			documentProcessTime.setGeojsonTime(geojsonEndTime - geojsonStartTime);
			
			if(geojsonList.isEmpty()){
				long updateStartTime = System.currentTimeMillis();
				Main.outputColl.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
						new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)
													.append(Main.configPropertyValues.geonameOutputField, geonameList)));
				long updateEndTime = System.currentTimeMillis();
				documentProcessTime.setMongoUpdateTime(updateEndTime - updateStartTime);
			}
			else{
				long updateStartTime = System.currentTimeMillis();
				Main.outputColl.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
						new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)
													.append(Main.configPropertyValues.geonameOutputField, geonameList)
													.append(Main.configPropertyValues.geojsonListOutputField, geojsonList.geometryCollection)));
				long updateEndTime = System.currentTimeMillis();
				documentProcessTime.setMongoUpdateTime(updateEndTime - updateStartTime);
			}
			
		}
		
		long docEndTime = System.currentTimeMillis();
		documentProcessTime.setDocumentProcessTime(docEndTime - docStartTime);
		
		Main.documentProcessTimeHandler.updateTotalTime(documentProcessTime);
		Main.documentProcessTimeHandler.incPeriodicTime(documentProcessTime);
		
		Main.timelyDocCount.incrementAndGet();
		if(documentCount % DocumentProcessTimeHandler.documentCountInterval == 0){
			LOGGER.info(documentCount + " documents has been processed. " + textEntitiesDocCount + " documents has entities from text. " + userEntitiesDocCount + " documents has entities from user profile location.");
			//Main.documentProcessTimeHandler.updateMongoForPeriodicDocumentProcessTime(documentCount);
			Main.documentProcessTimeHandler.resetPeriodicTime();
		}
	}
	
}

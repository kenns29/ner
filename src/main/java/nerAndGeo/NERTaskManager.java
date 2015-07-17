package nerAndGeo;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import util.CollUtilities;
import util.ErrorType;
import util.RetryCacheCollUtilities;
import util.TaskType;
import util.TimeRange;
import util.TimeUtilities;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class NERTaskManager implements Runnable{ 
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static final Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	
	private final BlockingQueue<TimeRange> queue;
	private DBCollection coll = null;
	private long startTime = 0;
	private long endTime = 0;
	private ObjectId startObjectId = null;
	private ObjectId endObjectId = null;
	
	//for toArray
	private final int CURSOR_RETRY_LIMIT = 2;
	//for query
	private final int CURSOR_RETRY_LIMIT1 = 6;
	public NERTaskManager(long startTime, long endTime, BlockingQueue<TimeRange> queue, DBCollection coll){
		this.startTime = startTime;
		this.endTime = endTime;
		this.startObjectId = TimeUtilities.getObjectId(startTime, 0, 0, 0);
		this.endObjectId = TimeUtilities.getObjectId(endTime, 0, 0, 0);
		this.queue = queue;
		this.coll = coll;
	}
	
	public NERTaskManager(ObjectId startObjectId, ObjectId endObjectId, BlockingQueue<TimeRange> queue, DBCollection coll){
		this.startObjectId = startObjectId;
		this.endObjectId = endObjectId;
		this.startTime = TimeUtilities.getTimestampFromObjectId(startObjectId);
		this.endTime = TimeUtilities.getTimestampFromObjectId(endObjectId);
		this.queue = queue;
		this.coll = coll;
	}
	
	private BasicDBObject constructQuery(ObjectId startObjectId, ObjectId endObjectId){
		BasicDBObject query = null;
		if(Main.configPropertyValues.stopAtEnd){
			if(Main.configPropertyValues.catID < 0){
				query = new BasicDBObject("_id", new BasicDBObject("$gt", startObjectId)
												.append("$lte", endObjectId));
			}
			else{
				query = new BasicDBObject("_id", new BasicDBObject("$gt", startObjectId)
													.append("$lte", endObjectId))
						.append("cat", Main.configPropertyValues.catID);
			}
		}
		else{
			if (Main.configPropertyValues.catID < 0){
				query = new BasicDBObject("_id", new BasicDBObject("$gt", startObjectId));
			}
			else{
				query = new BasicDBObject("_id", new BasicDBObject("$gt", startObjectId))
						.append("cat", Main.configPropertyValues.catID);
			}
			
		}
		return query;
	}
	public void run(){
		LOGGER.info("There are total of " + Main.configPropertyValues.core + " threads.");
		
		//Split Tasks by time interval
		if(Main.configPropertyValues.splitOption == 0){
			splitTasksByTimeInterval();
		}
		//Split tasks by number of documents
		else{
			splitTasksByNumDocuments();
		}
	}
	
	//Split Tasks by time interval
	private void splitTasksByTimeInterval(){
		if(Main.configPropertyValues.stopAtEnd){
			for(long t = TimeUtilities.hourFloor(startTime); t < TimeUtilities.hourCeiling(endTime); t+= Main.configPropertyValues.splitIntervalInMillis){
				try {
					queue.put(new TimeRange(t, t + Main.configPropertyValues.splitIntervalInMillis));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		else{
			long nextStartTime = TimeUtilities.minutesFloor(startTime);
			while(true){
				long nextEndTime = nextStartTime + Main.configPropertyValues.splitIntervalInMillis;
				ObjectId nextEndObjectId = TimeUtilities.getObjectId(nextEndTime, 0, 0, 0);
				ObjectId maxObjectId = CollUtilities.maxObjectId(this.coll);
				long maxTime = TimeUtilities.getTimestampFromObjectId(maxObjectId);
				
				while(nextEndObjectId.compareTo(maxObjectId) >= 0){
					long timeDiff = nextEndTime - maxTime;
					LOGGER.info("The query for " + TimeUtilities.js_timestampToString(nextStartTime) 
							+ " To " 
							+ TimeUtilities.js_timestampToString(nextEndTime) 
							+" reached the end, waiting for " + timeDiff + " milliseconds\n"
							+ "Current max time is " + TimeUtilities.js_timestampToString(maxTime)
							+ "\nGoing to Sleep.");
					try {
						Thread.sleep(timeDiff);
					} catch (InterruptedException e) {
						LOGGER.error("Task Manager Interrupted while sleeping", e);
					}
					LOGGER.info("Wake Up for " + TimeUtilities.js_timestampToString(nextStartTime) 
							+ " To " 
							+ TimeUtilities.js_timestampToString(nextEndTime));
					maxObjectId = CollUtilities.maxObjectId(this.coll);
					maxTime = TimeUtilities.getTimestampFromObjectId(maxObjectId);
				}
				
				TimeRange timeRange = new TimeRange(nextStartTime, nextEndTime);
				LOGGER.info("Inserting new time range " + timeRange.toString() + " to the queue");
				try {
					queue.put(timeRange);
					LOGGER.info("time range " + timeRange.toString() + " is inserted to the queue to the queue");
				} catch (InterruptedException e) {
					LOGGER.error("INSERTING " + timeRange.toString() + " is INTERRUPTED", e);
				}
				nextStartTime = nextEndTime;
			}
		}
	}
	
	//Split tasks by number of documents
	private void splitTasksByNumDocuments(){
		
		
		ObjectId nextStartObjectId = TimeUtilities.decrementObjectId(this.startObjectId);
		boolean continueFlag = true;
		boolean waitFlag = false;
		int waitTime = 60000; //1 min
		while(continueFlag){
			long taskManagerStartTime = System.currentTimeMillis();
		
			//Queue Tasks on Retry Cache
			boolean startMainData = false;
			if(Main.retryCacheAvailable && Main.geonameServiceAvailable){
				BasicDBObject field = new BasicDBObject("_id", 1)
				.append("coordinates", 1)
				.append("created_at", 1)
				.append("id", 1)
				.append("text", 1)
				.append("timestamp", 1)
				.append("place", 1)
				.append("location", 1)
				.append("user.location", 1)
				.append(RetryCacheCollUtilities.ERROR_TYPE_FIELD_NAME, 1)
				.append(RetryCacheCollUtilities.ERROR_COUNT_FIELD_NAME, 1);
				
				BasicDBList orList = new BasicDBList();
				orList.add(new BasicDBObject(RetryCacheCollUtilities.ERROR_TYPE_FIELD_NAME, ErrorType.SOCKET_TIME_OUT));
				orList.add(new BasicDBObject(RetryCacheCollUtilities.ERROR_TYPE_FIELD_NAME, ErrorType.FILE_NOT_FOUND));
				orList.add(new BasicDBObject(RetryCacheCollUtilities.ERROR_TYPE_FIELD_NAME, ErrorType.CONNECT_EXCEPTION));
				BasicDBObject query = new BasicDBObject(new BasicDBObject("$or", orList));
				
				ArrayList<DBObject> mongoObjList = null;
				DBCursor cursor = null;
				try{
					cursor = Main.retryCacheColl.find(query, field).sort(new BasicDBObject("_id", 1)).limit(Main.configPropertyValues.numDocsInThread);
				}
				catch(Exception e){
					String msg = "Query for retry cache did not succeed."
							+ "\nDue to Unexpected Error during the query.";
					
					LOGGER.fatal(msg, e);
				}
				
				if(cursor != null){
					long countStartTime = System.currentTimeMillis();
					int cCount = cursor.count();
					long countEndTime = System.currentTimeMillis();
					LOGGER.info("Counted Cursor for the retry task, it took " + (countEndTime - countStartTime) + " milliseconds.");
					if(cCount > 0){
						LOGGER.info("Query for retry Cache, there are total of " + cCount + " items");
						try{
							mongoObjList = (ArrayList<DBObject>) cursor.toArray();
							LOGGER.info("Successfully converted Cursor for retry cache to list.");
						}
						catch(Exception e){
							String msg = "Did not succeed in converting the cursor to array for retry cache."
									+ "\nDue to Unexpected Error during the query.";
							
							LOGGER.fatal(msg, e);
						}
						finally{
							cursor.close();
						}
					}
					else{
						startMainData = true;
					}
				}
				else{
					startMainData = true;
				}
				
				if(mongoObjList != null && mongoObjList.size() > 0){
					BasicDBObject startObject = (BasicDBObject) mongoObjList.get(0);
					BasicDBObject endObject = (BasicDBObject) mongoObjList.get(mongoObjList.size() - 1);
					
					ObjectId startObjectId = startObject.getObjectId("_id");
					ObjectId endObjectId = endObject.getObjectId("_id");
					
					TimeRange timeRange = new TimeRange(startObjectId, endObjectId, mongoObjList, TaskType.RETRY_TASK);
					
					try {
						Main.retryCacheAvailable = false;
						queue.put(timeRange);
						LOGGER.info("Object id range " + timeRange.toObjectIdString() + ", with time range " + timeRange.toString() + " is inserted to the queue.");
					} catch (InterruptedException e) {
						LOGGER.error("INSERTING Object id range " + timeRange.toObjectIdString() + ", with time range " +  timeRange.toString() + " is INTERRUPTED", e);
					}
				}
				
			}
			else{
				startMainData = true;
			}
		
			//Queue Tasks on the main data
			if(startMainData){
				BasicDBObject field = new BasicDBObject("_id", 1)
				.append("coordinates", 1)
				.append("created_at", 1)
				.append("id", 1)
				.append("text", 1)
				.append("timestamp", 1)
				.append("place", 1)
				.append("location", 1)
				.append("user.location", 1);
				
				waitFlag = false;
				ObjectId startObjectId = nextStartObjectId;
				BasicDBObject query = this.constructQuery(nextStartObjectId, this.endObjectId);
	
				boolean retryFlag = false;
				int unexpectedExceptionCount = 0;
				ArrayList<DBObject> mongoObjList = null;
				
				
				do{
					//Query the documents
					DBCursor cursor = null;
					boolean retryFlag1 = false;
					int unexpectedExceptionCount1 = 0;
					do{
						try{
							long systime = System.currentTimeMillis();
							cursor = coll.find(query, field).sort(new BasicDBObject("_id", 1)).limit(Main.configPropertyValues.numDocsInThread);
							long newSystime = System.currentTimeMillis();
							retryFlag1 = false;
							LOGGER.info("Query for "+ nextStartObjectId.toHexString() + ", query took " + (newSystime - systime) + " milliseconds.");
						}
						catch(Exception e){
							++unexpectedExceptionCount1;
							if(unexpectedExceptionCount1 > this.CURSOR_RETRY_LIMIT1){
								retryFlag1 = false;
								unexpectedExceptionCount1 = 0;
								String msg = "Time Range starts with " + nextStartObjectId.toHexString() + " has not been successfully inserted to the queue."
										+ "\nDue to Unexpected Error during the query.";
								
								HIGH_PRIORITY_LOGGER.fatal(msg, e);
							}
							else{
								retryFlag1 = true;
							}
						}
					}
					while(retryFlag1);
					
					//Converting the cursor to array
					if(cursor != null){
						cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
			
//						long countStartTime = System.currentTimeMillis();
//						int cCount = cursor.count();
//						long countEndTime = System.currentTimeMillis();
//						LOGGER.info("Counted Cursor for the normal task, it took " + (countEndTime - countStartTime) + " milliseconds.");
//						//If the cursor has fewer documents than numDocsInThread + 1
//						if(cCount < Main.configPropertyValues.numDocsInThread){
//							if(Main.configPropertyValues.stopAtEnd){
//								continueFlag = false;
//							}
//							else{
//								waitFlag = true;
//							}
//						}
						
						try{
							long systime = System.currentTimeMillis();
							mongoObjList = (ArrayList<DBObject>) cursor.toArray();
							long newSystime = System.currentTimeMillis();
							
							int cCount = mongoObjList.size();
							
							if(cCount < Main.configPropertyValues.numDocsInThread){
								if(Main.configPropertyValues.stopAtEnd){
									continueFlag = false;
								}
								else{
									waitFlag = true;
								}
							}
							retryFlag = false;
							LOGGER.info("Converted cursor to array for " + nextStartObjectId.toHexString() + ", it took " + (newSystime - systime) + " milliseconds.");
						}
						catch(Exception e){
							++unexpectedExceptionCount;
							if(unexpectedExceptionCount > this.CURSOR_RETRY_LIMIT){
								retryFlag = false;
								unexpectedExceptionCount = 0;
								String msg = "Time Range starts with " + nextStartObjectId.toHexString() + " has not been successfully inserted to the queue."
										+ "\nDue to Unexpected Error while converting the cursor to array.";
								
								HIGH_PRIORITY_LOGGER.fatal(msg, e);
							}
							else{
								retryFlag = true;
							}
						}
						finally{
							cursor.close();
						}
					}
				}
				while(retryFlag);
				
				//put the task into the queue
				if(mongoObjList != null && mongoObjList.size() > 0){ 
					BasicDBObject nextStartObj = (BasicDBObject) mongoObjList.get(mongoObjList.size() - 1);
					
					ObjectId nextEndObjectId = nextStartObj.getObjectId("_id");
					TimeRange timeRange = new TimeRange(nextStartObjectId, nextEndObjectId, mongoObjList, TaskType.NORMAL_TASK);
					
					long taskManagerEndTime = System.currentTimeMillis();
					
					synchronized(this){
						Main.taskMangerFinishCount.incrementAndGet();
						Main.totalTaskManagerFinishedTime += (taskManagerEndTime - taskManagerStartTime);
					}	
			
					LOGGER.info("Task Manager Successfully prepared the task, it took " + (taskManagerEndTime - taskManagerStartTime) + " milliseconds.");

					try {
						queue.put(timeRange);
						LOGGER.info("Object id range " + timeRange.toObjectIdString() + ", with time range " + timeRange.toString() + " is inserted to the queue.");
					} catch (InterruptedException e) {
						LOGGER.error("INSERTING Object id range " + timeRange.toObjectIdString() + ", with time range " +  timeRange.toString() + " is INTERRUPTED", e);
					}
					nextStartObjectId = nextStartObj.getObjectId("_id");
				}
				//Query did not succeed, need to pick an arbitrary next starting point
				else if(mongoObjList == null){
					long nextStartTime = TimeUtilities.getTimestampFromObjectId(nextStartObjectId);
					String msg = "Query for starting point " + nextStartObjectId.toHexString() + " did not succeed. ";
					nextStartObjectId = TimeUtilities.getObjectId(nextStartTime + 300000, 0, 0, 0); 
					msg += "Picking an arbitrary Object ID " + nextStartObjectId.toHexString() + " as next starting point.";
					HIGH_PRIORITY_LOGGER.fatal(msg);
				}
				
				if(waitFlag){
					LOGGER.info("The query for " + startObjectId.toHexString() 
							+" has almost reached the end, waiting for " + waitTime + " milliseconds."
							+ "\nGoing to Sleep and wait for " + nextStartObjectId.toHexString() + ".");
					
					try {
						Thread.sleep(waitTime);
					} catch (InterruptedException e) {
						LOGGER.error("Task Manager Interrupted while sleeping", e);
					}
					
					LOGGER.info("Wake Up for old task at" + startObjectId.toHexString() + ", starting a new task at " + nextStartObjectId.toHexString());
				}
			}
		}
	}
}

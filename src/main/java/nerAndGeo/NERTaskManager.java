package nerAndGeo;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import util.CollUtilities;
import util.TimeRange;
import util.TimeUtilities;

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
	//maximum time a task can wait
	private final int MAX_WAIT_TIME = 180000; //3 min
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
	public void run(){
		LOGGER.info("There are total of " + Main.configPropertyValues.core + " threads.");
		
		//Split Tasks by time interval
		if(Main.configPropertyValues.splitOption == 0){
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
		else{
			ObjectId nextStartObjectId = TimeUtilities.decrementObjectId(this.startObjectId);
			boolean continueFlag = true;
			boolean skipFlag = false;
			int waitTimeCount = 0;
			while(continueFlag){
				skipFlag = false;
				BasicDBObject query = null;
				if(Main.configPropertyValues.stopAtEnd){
					query = new BasicDBObject("_id", new BasicDBObject("$gt", nextStartObjectId)
													.append("$lte", this.endObjectId));
				}
				else{
					query = new BasicDBObject("_id", new BasicDBObject("$gt", nextStartObjectId));
				}
				BasicDBObject field = new BasicDBObject("_id", 1)
				.append("coordinates", 1)
				.append("created_at", 1)
				.append("id", 1)
				.append("text", 1)
				.append("timestamp", 1)
				.append("place", 1)
				.append("location", 1)
				.append("user.location", 1);
				
				boolean retryFlag = false;
				int unexpectedExceptionCount = 0;
				ArrayList<DBObject> mongoObjList = null;
				do{
					DBCursor cursor = null;
					boolean retryFlag1 = false;
					int unexpectedExceptionCount1 = 0;
					//Query the documents
					do{
						try{
							cursor = coll.find(query, field).sort(new BasicDBObject("_id", 1)).limit(Main.configPropertyValues.numDocsInThread);
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
						//If the cursor has fewer documents than numDocsInThread + 1
						if(cursor.count() < Main.configPropertyValues.numDocsInThread){
							if(Main.configPropertyValues.stopAtEnd){
								continueFlag = false;
							}
							else{
								long timeDiff = 60000; //wait 1min
								LOGGER.info("The query for " + nextStartObjectId.toHexString() 
										+" reached the end, waiting for " + timeDiff + " milliseconds."
										+ "\nGoing to Sleep.");
								
								if(waitTimeCount < this.MAX_WAIT_TIME){
									waitTimeCount += timeDiff;
									try {
										Thread.sleep(timeDiff);
									} catch (InterruptedException e) {
										LOGGER.error("Task Manager Interrupted while sleeping", e);
									}
									
									LOGGER.info("Wake Up for task starting at " + nextStartObjectId.toHexString());
									cursor.close();
									skipFlag = true;
									break; 
								}
								else{
									waitTimeCount = 0;
								}
							}
						}
						
						try{
							mongoObjList = (ArrayList<DBObject>) cursor.toArray();
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
				if(mongoObjList != null && mongoObjList.size() > 0 && !skipFlag){ 
					BasicDBObject nextStartObj = (BasicDBObject) mongoObjList.get(mongoObjList.size() - 1);
					
					ObjectId nextEndObjectId = nextStartObj.getObjectId("_id");
					TimeRange timeRange = new TimeRange(nextStartObjectId, nextEndObjectId, mongoObjList);
					try {
						queue.put(timeRange);
						LOGGER.info("Object id range " + timeRange.toObjectIdString() + ", with time range " + timeRange.toString() + " is inserted to the queue to the queue");
					} catch (InterruptedException e) {
						LOGGER.error("INSERTING Object id range " + timeRange.toObjectIdString() + ", with time range " +  timeRange.toString() + " is INTERRUPTED", e);
					}
					nextStartObjectId = nextStartObj.getObjectId("_id");
				}
				//Query did not succeed, need to pick an arbitrary next starting point
				else if(mongoObjList == null && !skipFlag){
					long nextStartTime = TimeUtilities.getTimestampFromObjectId(nextStartObjectId);
					String msg = "Query for starting point " + nextStartObjectId.toHexString() + " did not succeed. ";
					nextStartObjectId = TimeUtilities.getObjectId(nextStartTime + 300000, 0, 0, 0); 
					msg += "Picking an arbitrary Object ID " + nextStartObjectId.toHexString() + " as next starting point.";
					HIGH_PRIORITY_LOGGER.fatal(msg);
				}
			}
		}
	}
}

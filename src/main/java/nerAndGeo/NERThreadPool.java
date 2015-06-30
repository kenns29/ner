package nerAndGeo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class NERThreadPool {
	private static final Logger LOGGER = Logger.getLogger(NERThreadPool.class.getName());
	static{
		LOGGER.addHandler(LoggerAttr.fileHandler);
	}
	
	private int numOfThreads = 1;
	private DBCollection coll = null;
	private String inputField = null;
	private long startTime = 0;
	private long endTime = 0;
	private long nextStartTime = 0;
	private int nextMachineId = 0;
	private int nextPid = 0;
	private int nextSerialNumber = 0;
	private ObjectId nextObjectId = null;
	public static int count = 0;
	public static long preTime = System.currentTimeMillis();
	public NERThreadPool(){
	}
	
	public NERThreadPool(DBCollection coll, String inputField, int numOfThreads){
		this.coll = coll;
		this.inputField = inputField;
		this.numOfThreads = numOfThreads;
	}
	public NERThreadPool(int numOfThreads){
		this.numOfThreads = numOfThreads;
	}
	public NERThreadPool(DBCollection coll, String inputField, int numOfThreads, long startTime, long endTime){
		this(coll, inputField, numOfThreads);
		this.startTime = startTime;
		this.endTime = endTime;
	}
	
	public void run(){
		LOGGER.info("There are total of " + numOfThreads + " threads.");
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		if(Main.configPropertyValues.splitOption == 0){
			if(Main.configPropertyValues.stopAtEnd){
				for(long t = TimeUtilities.hourFloor(startTime); t < TimeUtilities.hourCeiling(endTime); t+= Main.configPropertyValues.splitIntervalInMillis){
					Runnable worker = new NERThread(coll, inputField, t, t+Main.configPropertyValues.splitIntervalInMillis);
					executor.execute(worker);
				}
			}
			else{
				nextStartTime = TimeUtilities.minutesFloor(startTime);
				LOGGER.info("The first startTime = " + TimeUtilities.js_timestampToString(nextStartTime));
				while(true){
					LOGGER.info("The startTime = " + TimeUtilities.js_timestampToString(nextStartTime));
					long nextEndTime = nextStartTime + Main.configPropertyValues.splitIntervalInMillis;
					ObjectId nextEndObjectId = TimeUtilities.getObjectId(nextEndTime, 0, 0, 0);
					
					//Wait for the maxObjectId catches up with the nextEndObjectId
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
							e.printStackTrace();
						}
						LOGGER.info("Wake Up for " + TimeUtilities.js_timestampToString(nextStartTime) 
								+ " To " 
								+ TimeUtilities.js_timestampToString(nextEndTime));
						maxObjectId = CollUtilities.maxObjectId(this.coll);
						maxTime = TimeUtilities.getTimestampFromObjectId(maxObjectId);
					}
					
				
					LOGGER.info("Starting new Thread for (" + TimeUtilities.js_timestampToString(nextStartTime) + " To " + TimeUtilities.js_timestampToString(nextEndTime) + ").");
					Runnable worker = new NERThread(coll, inputField, nextStartTime, nextEndTime);
					executor.execute(worker);
					nextStartTime = nextEndTime;
				
					
				}
			}
		}
		else{
			nextStartTime = startTime;
			nextObjectId = TimeUtilities.getObjectId(nextStartTime, 0, 0, 0);
			do{
				BasicDBObject query = new BasicDBObject("_id", new BasicDBObject("$gte", nextObjectId));
				
			}while(true);
		}
		executor.shutdown();
		while(!executor.isTerminated()){}
		LOGGER.info("all threads finished.");
	}
	
}

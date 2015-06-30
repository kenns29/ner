package nerAndGeo;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import org.bson.types.ObjectId;

import util.CollUtilities;
import util.TimeRange;
import util.TimeUtilities;

import com.mongodb.DBCollection;

public class NERTaskManager implements Runnable{
	private static final Logger LOGGER = Logger.getLogger(NERTaskManager.class.getName());
	static{
		LOGGER.addHandler(LoggerAttr.fileHandler);
	}
	
	private final BlockingQueue<TimeRange> queue;
	private DBCollection coll = null;
	private long startTime = 0;
	private long endTime = 0;
	
	public static int count = 0;
	public static long preTime = System.currentTimeMillis();
	
	public NERTaskManager(long startTime, long endTime, BlockingQueue<TimeRange> queue, DBCollection coll){
		this.startTime = startTime;
		this.endTime = endTime;
		this.queue = queue;
		this.coll = coll;
	}
	
	public void run(){
		LOGGER.info("There are total of " + Main.configPropertyValues.core + " threads.");
		
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
			LOGGER.info("The first startTime = " + TimeUtilities.js_timestampToString(nextStartTime));
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
						e.printStackTrace();
					}
					LOGGER.info("Wake Up for " + TimeUtilities.js_timestampToString(nextStartTime) 
							+ " To " 
							+ TimeUtilities.js_timestampToString(nextEndTime));
					maxObjectId = CollUtilities.maxObjectId(this.coll);
					maxTime = TimeUtilities.getTimestampFromObjectId(maxObjectId);
					
					TimeRange timeRange = new TimeRange(nextStartTime, nextEndTime);
					LOGGER.info("Inserting new time range " + timeRange.toString() + " to the queue");
					try {
						queue.put(timeRange);
					} catch (InterruptedException e) {
						LOGGER.severe("INSERTING " + timeRange.toString() + " is INTERRUPTED");
						e.printStackTrace();
					}
					nextStartTime = nextEndTime;
				}
			}
		}
	}
}

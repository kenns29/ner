package nerAndGeo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

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
		for(long t = TimeUtilities.dayFloor(startTime); t < TimeUtilities.dayCeiling(endTime); t+= TimeUtilities.dayMillis){
			Runnable worker = new NERThread(coll, inputField, t, t+TimeUtilities.dayMillis);
			executor.execute(worker);
		}
		executor.shutdown();
		while(!executor.isTerminated()){}
		LOGGER.info("all threads finished.");
	}
	
}

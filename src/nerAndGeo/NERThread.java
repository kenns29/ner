package nerAndGeo;

import java.util.logging.Logger;

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
	private BasicDBObject query = null;
	private long startTime = 0;
	private long endTime = 0;
	public NERThread(){
		super();
	}
	public NERThread(DBCollection coll, String inputField, long startTime, long endTime){
		this.coll = coll;
		this.inputField = inputField;
		this.startTime = startTime;
		this.endTime = endTime;
		//use the catID
		if(Main.configPropertyValues.catID < 0){
			switch(Main.configPropertyValues.parallelFlag){
			case 1:
				this.query = new BasicDBObject("_id", new BasicDBObject("$gte", TimeUtilities.getObjectIdFromTimestamp(this.startTime))
															.append("$lt", TimeUtilities.getObjectIdFromTimestamp(this.endTime)));
				break;
			case 0:
			default:
				this.query = new BasicDBObject("timestamp", 
									new BasicDBObject("$gte", this.startTime)
									.append("$lt", this.endTime));
			}
			
		}
		//do not use the cat ID
		else{
			switch(Main.configPropertyValues.parallelFlag){
			case 1:
				this.query = new BasicDBObject("cat", Main.configPropertyValues.catID)
							.append("_id", new BasicDBObject("$gte", TimeUtilities.getObjectIdFromTimestamp(this.startTime))
											.append("$lt", TimeUtilities.getObjectIdFromTimestamp(this.endTime)));
				break;
			case 0:
			default:
				this.query = new BasicDBObject("cat", Main.configPropertyValues.catID)
									.append("timestamp", 
										new BasicDBObject("$gte", this.startTime)
										.append("$lt", this.endTime));
			}
		}
	}
	@Override
	public void run() {
		LOGGER.info("Starting new Thread for (StartTime " + startTime + ", endTime " + endTime + ")");
		LOGGER.info("equivalent to from ObjectId " + TimeUtilities.getObjectIdFromTimestamp(startTime) + " to " + TimeUtilities.getObjectIdFromTimestamp(endTime));
		StanfordCoreNLP pipeline = new StanfordCoreNLP(Main.NLPprops);
		long start = System.currentTimeMillis();
		try {
			insertNer(pipeline, Main.configPropertyValues.geoname);
		} catch (Exception e) {
			LOGGER.warning("ner parsing error");
			e.printStackTrace();
		}
		long time = System.currentTimeMillis() - start;
		LOGGER.info("FinishThread for (StartTime " + startTime + ", endTime " + endTime + "). Elapsed Time = " + time
		+ "\nequivalent to from ObjectId " + TimeUtilities.getObjectIdFromTimestamp(startTime) + " to " + TimeUtilities.getObjectIdFromTimestamp(endTime));
	}
	
	public void insertNer(StanfordCoreNLP pipeline, boolean useGeoname) throws Exception{
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		LOGGER.info("Querying for (StartTime " + startTime + ", endTime " + endTime + "), there are total of " + cursor.count() + " items"
			+ "\nequivalent to from ObjectId " + TimeUtilities.getObjectIdFromTimestamp(startTime) + " to " + TimeUtilities.getObjectIdFromTimestamp(endTime));
		if(useGeoname){
			LOGGER.info("inserting entities along with geonames");
		}
		else{
			LOGGER.info("inserting entities");
		}
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				if(text != null && text.length() < 1000){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					BasicDBList entities = NER.annotateDBObject(text, pipeline);
					
					if(!useGeoname){
						coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
											new BasicDBObject("$set", new BasicDBObject("ner", entities)));
					}
					else{
						BasicDBList geonameList = Geoname.getGeonameList(entities);
						coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
											new BasicDBObject("$set", new BasicDBObject("ner", entities)
																		.append("geoname", geonameList)));
					}
					
				}
				
				++NERThreadPool.count;
				long time = System.currentTimeMillis();
				if(time - NERThreadPool.preTime >= 60000){
					LOGGER.info("From " + NERThreadPool.preTime + " to " + time + ", " + NERThreadPool.count + " are processed. The time range is " + (time - NERThreadPool.preTime));
					NERThreadPool.preTime = time;
					NERThreadPool.count = 0;
				}
			}
		}
		finally{
			cursor.close();
		}
	}
	public void insertNer(boolean useGeoname) throws Exception{
		insertNer(Main.pipeline, useGeoname);
	}
	
	
}

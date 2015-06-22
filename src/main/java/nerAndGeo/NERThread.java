package nerAndGeo;

import java.util.Properties;
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
	private String startTimeStr = null;
	private String endTimeStr = null;
	public NERThread(){
		super();
	}
	public NERThread(DBCollection coll, String inputField, long startTime, long endTime){
		this.coll = coll;
		this.inputField = inputField;
		this.startTime = startTime;
		this.endTime = endTime;
		this.startTimeStr = TimeUtilities.js_timestampToString(startTime);
		this.endTimeStr = TimeUtilities.js_timestampToString(endTime);
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
		LOGGER.info("Starting new Thread for (StartTime " + startTimeStr + ", endTime " + endTimeStr + ")\n" 
				+ "equivalent to from ObjectId " + TimeUtilities.getObjectIdFromTimestamp(startTime) + " to " + TimeUtilities.getObjectIdFromTimestamp(endTime));
		Properties NLPprops = new Properties();
		NLPprops.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(NLPprops);
		long start = System.currentTimeMillis();
		try {
			insertNer(pipeline);
		} catch (Exception e) {
			LOGGER.warning("ner parsing error");
			e.printStackTrace();
		}
		long time = System.currentTimeMillis() - start;
		LOGGER.info("Finished Thread for (StartTime " + startTimeStr + ", endTime " + endTimeStr + "). Elapsed Time = " + time
		+ "\nequivalent to from ObjectId " + TimeUtilities.getObjectIdFromTimestamp(startTime) + " to " + TimeUtilities.getObjectIdFromTimestamp(endTime));
	}
	
	public void insertNer(StanfordCoreNLP pipeline) throws Exception{
		DBCursor cursor = coll.find(query).sort(new BasicDBObject("_id", 1));
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		LOGGER.info("Querying for (StartTime " + startTimeStr + ", endTime " + endTimeStr + "), there are total of " + cursor.count() + " items"
			+ "\nequivalent to from ObjectId " + TimeUtilities.getObjectIdFromTimestamp(startTime) + " to " + TimeUtilities.getObjectIdFromTimestamp(endTime));
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
						System.out.println("userText = " + userText);
						userEntities = NER.annotateDBObject(userText, pipeline, startTimeStr, endTimeStr);
						userEntities = NER.insertFromFlag(userEntities, "user.location");
					}
				}
				
				if(text != null && text.length() < 1000){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					textEntities = NER.annotateDBObject(text, pipeline, startTimeStr, endTimeStr);
					textEntities = NER.insertFromFlag(textEntities, Main.configPropertyValues.nerInputField);
				}
				
				BasicDBList entities = new BasicDBList();
				if(textEntities != null)
					entities.addAll(textEntities);
				if(userEntities != null)
					entities.addAll(userEntities);
				
				if(!Main.configPropertyValues.geoname){
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
							new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)));
				}
				else if(Main.configPropertyValues.outputOption == 0){
					BasicDBList nerGeonameList = Geoname.makeNerGeonameList(entities);
//					System.out.println(nerGeonameList.toString());
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
							new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, nerGeonameList)));
				}
				else if(Main.configPropertyValues.outputOption == 1){
					BasicDBList geonameList = Geoname.makeGeonameList(entities);
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
							new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)
														.append(Main.configPropertyValues.geonameOutputField, geonameList)));
				}
				++NERThreadPool.count;
				long time = System.currentTimeMillis();
				if(time - NERThreadPool.preTime >= 60000){
					LOGGER.info("From " + NERThreadPool.preTime + " to " + time + ", " + NERThreadPool.count + " are processed. The time range is " + (time - NERThreadPool.preTime));
					NERThreadPool.preTime = time;
					NERThreadPool.count = 0;
				}
				
				if(Main.documentCount % 1000 == 0){
					LOGGER.info(Main.documentCount + "documents has been processed.");
				}
			}
		}
		finally{
			cursor.close();
		}
	}	
}

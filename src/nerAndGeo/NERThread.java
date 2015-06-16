package nerAndGeo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class NERThread implements Runnable{
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
		this.query = new BasicDBObject("cat", Main.configPropertyValues.catID)
						.append("timestamp", 
								new BasicDBObject("$gte", this.startTime)
								.append("$lt", this.endTime));
	}
	@Override
	public void run() {
		System.out.println("Starting new Thread for (StartTime " + startTime + ", endTime " + endTime + ")");
		long start = System.currentTimeMillis();
		try {
			insertNer(Main.configPropertyValues.geoname);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("FinishThread for (StartTime " + startTime + ", endTime " + endTime + "). Elapsed Time = " + time);
	}
	
	public void insertNer(boolean useGeoname) throws Exception{
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("Querying for (StartTime " + startTime + ", endTime " + endTime + "), there are total of " + cursor.count() + " items");
		if(useGeoname){
			System.out.println("inserting entities along with geonames");
		}
		else{
			System.out.println("inserting entities");
		}
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				if(text != null && text.length() < 1000){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					BasicDBList entities = NLP.annotateDBObject(text);
					
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
					System.out.println("From " + NERThreadPool.preTime + " to " + time + ", " + NERThreadPool.count + " are processed. The time range is " + (time - NERThreadPool.preTime));
					NERThreadPool.preTime = time;
					NERThreadPool.count = 0;
				}
			}
		}
		finally{
			cursor.close();
		}
	}
	
	
}

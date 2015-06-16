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
		this.query = new BasicDBObject("timestamp", 
				new BasicDBObject("$gte", this.startTime)
				.append("$lt", this.endTime));
	}
	@Override
	public void run() {
		System.out.println("Starting new Thread for (StartTime " + startTime + ", endTime " + endTime + ")");
		long start = System.currentTimeMillis();
		insertNer();
		long time = System.currentTimeMillis() - start;
		System.out.println("FinishThread for (StartTime " + startTime + ", endTime " + endTime + "). Elapsed Time = " + time);
	}
	
	public void insertNer(){
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("Querying for (StartTime " + startTime + ", endTime " + endTime + "), there are total of " + cursor.count() + " items");
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				if(text != null && text.length() < 1000){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					BasicDBList entities = NLP.annotateDBObject(text);
					
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
										new BasicDBObject("$set", new BasicDBObject("ner", entities)));
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

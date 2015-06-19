package nerAndGeo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class CollUtilities {
	public static long minTime(DBCollection coll){
		DBCursor cursor = coll.find().sort(new BasicDBObject("timestamp", 1)).limit(1);
		if(cursor.hasNext()){
			BasicDBObject mongoObj = (BasicDBObject) cursor.next();
			return mongoObj.getLong("timestamp");
		}
		return 0;
	}
	
	public static long maxTime(DBCollection coll){
		DBCursor cursor = coll.find().sort(new BasicDBObject("timestamp", -1)).limit(1);
		if(cursor.hasNext()){
			BasicDBObject mongoObj = (BasicDBObject) cursor.next();
			return mongoObj.getLong("timestamp");
		}
		return 0;
	}
}

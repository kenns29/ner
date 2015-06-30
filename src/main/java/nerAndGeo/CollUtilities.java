package nerAndGeo;

import org.bson.types.ObjectId;

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
	
	public static ObjectId minObjectId(DBCollection coll){
		DBCursor cursor = coll.find().sort(new BasicDBObject("_id", 1)).limit(1);
		if(cursor.hasNext()){
			BasicDBObject mongoObj = (BasicDBObject) cursor.next();
			return mongoObj.getObjectId("_id");
		}
		return null;	
	}
	
	public static ObjectId maxObjectId(DBCollection coll){
		DBCursor cursor = coll.find().sort(new BasicDBObject("_id", -1)).limit(1);
		if(cursor.hasNext()){
			BasicDBObject mongoObj = (BasicDBObject) cursor.next();
			return mongoObj.getObjectId("_id");
		}
		return null;
	}
	
	public static long minInsertionTime(DBCollection coll){
		ObjectId objectId = minObjectId(coll);
		if(objectId != null){
			return objectId.getDate().getTime();
		}
		return 0;
	}
	
	public static long maxInsertionTime(DBCollection coll){
		ObjectId objectId = maxObjectId(coll);
		if(objectId != null){
			return objectId.getDate().getTime();
		}
		return 0;
	}
	
	public static long minJsInsertionTime(DBCollection coll){
		return minInsertionTime(coll) * 1000;
	}
	
	public static long maxJsInsertionTime(DBCollection coll){
		return maxInsertionTime(coll) * 1000;
	}
}

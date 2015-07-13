package util;

import nerAndGeo.Main;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class RetryCacheCollUtilities {
	public static void insert(DBCollection coll, BasicDBObject mongoObj, ErrorType errorType){
		ObjectId objectId = mongoObj.getObjectId("_id");
		BasicDBObject setObj = copyObj(mongoObj);
		setObj.append("errorType", errorType.getType());
		coll.update(new BasicDBObject("_id", objectId), setObj, true, false);
	}
	
	public static void remove(DBCollection coll, ObjectId objectId){
		BasicDBObject query = new BasicDBObject("_id", objectId);
		coll.remove(query);
	}
	
	public static void remove(DBCollection coll, BasicDBObject mongoObj){
		ObjectId objectId = mongoObj.getObjectId("_id");
		remove(coll, objectId);
	}
	private static BasicDBObject copyObj(BasicDBObject mongoObj){
		BasicDBObject setObj = new BasicDBObject();
		setObj.append("_id", mongoObj.getObjectId("_id"));
		setObj.append("coordinates", mongoObj.get("coordinates"));
		setObj.append("created_at", mongoObj.getString("created_at"));
		setObj.append("id", mongoObj.getLong("id"));
		setObj.append("text", mongoObj.getString("text"));
		setObj.append("timestamp", mongoObj.getLong("timestamp"));
		setObj.append("place", mongoObj.get("place"));
		setObj.append("location", mongoObj.get("location"));
		setObj.append("user", mongoObj.get("user"));
		
		return setObj;
	}
}

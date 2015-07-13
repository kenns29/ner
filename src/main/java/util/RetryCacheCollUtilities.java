package util;

import nerAndGeo.Main;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class RetryCacheCollUtilities {
	private static Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	
	public static final String ERROR_TYPE_FIELD_NAME = "errorType";
	public static final String ERROR_COUNT_FIELD_NAME = "errorCount";
	public static final String ERROR_MESSAGE_FIELD_NAME = "exceptionStackTrace";
	
	public static void insert(DBCollection coll, BasicDBObject mongoObj, ErrorStatus errorStatus){
		ObjectId objectId = mongoObj.getObjectId("_id");
		BasicDBObject setObj = copyObj(mongoObj);
		setObj.append(ERROR_TYPE_FIELD_NAME, errorStatus.errorType.getType());
		setObj.append(ERROR_COUNT_FIELD_NAME, errorStatus.getErrorCount());
		setObj.append(ERROR_MESSAGE_FIELD_NAME, errorStatus.errorMsg);
		coll.update(new BasicDBObject("_id", objectId), new BasicDBObject("$set", setObj), true, false);
	}
	public static void remove(DBCollection coll, ObjectId objectId){
		BasicDBObject query = new BasicDBObject("_id", objectId);
		coll.remove(query);
	}
	
	public static void remove(DBCollection coll, BasicDBObject mongoObj){
		ObjectId objectId = mongoObj.getObjectId("_id");
		remove(coll, objectId);
	}
	
	public static void update(DBCollection coll, BasicDBObject mongoObj, ErrorStatus errorStatus){
		ObjectId objectId = mongoObj.getObjectId("_id");
		BasicDBObject setObj = copyObj(mongoObj);
		setObj.append(ERROR_TYPE_FIELD_NAME, errorStatus.errorType.getType());
		setObj.append(ERROR_COUNT_FIELD_NAME, errorStatus.getErrorCount());
		setObj.append(ERROR_MESSAGE_FIELD_NAME, errorStatus.errorMsg);
		coll.update(new BasicDBObject("_id", objectId), new BasicDBObject("$set", setObj));
	}
	
	public static ErrorStatus updateErrorTypeOrCount(DBCollection coll, BasicDBObject mongoObj, int errorType, Exception e){
		ErrorStatus errorStatus = RetryCacheCollUtilities.getErrorStatus(coll, mongoObj);
		if(errorStatus.errorType.getType() == errorType){
			errorStatus.incErrorCount();
			errorStatus.errorMsg = ExceptionUtils.getStackTrace(e);
			RetryCacheCollUtilities.update(coll, mongoObj, errorStatus);
		}
		else{
			errorStatus.errorType = new ErrorType(errorType);
			errorStatus.zeroErrorCount();
			errorStatus.errorMsg = ExceptionUtils.getStackTrace(e);
			RetryCacheCollUtilities.update(coll, mongoObj, errorStatus);
		}
		return errorStatus;
	}
	
	public static ErrorStatus getErrorStatus(DBCollection coll, BasicDBObject mongoObj){
		int errorType = mongoObj.getInt(ERROR_TYPE_FIELD_NAME);
		int errorCount = mongoObj.getInt(ERROR_COUNT_FIELD_NAME);
		return new ErrorStatus(new ErrorType(errorType), errorCount);
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

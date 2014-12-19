package scotlandTestData;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class Main {
	
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		Database database = new Database("fsdb1.dtn.asu.edu", 27017);
		database.getDatabase("foresight");
		DBCollection sentenceColl = database.getCollection("sentence");
		
		BasicDBObject query = new BasicDBObject("ner", null);
		DBCursor cursor = sentenceColl.find(query);
		
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString("sentence");
				BasicDBList entities = NLP.annotateDBObject(text);
				
				sentenceColl.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
									new BasicDBObject("$set", new BasicDBObject("ner", entities)));
			}
		}
		finally{
			cursor.close();
		}
	}

}

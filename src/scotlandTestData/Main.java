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
import com.mongodb.DBCursor;

public class Main {
	
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		Database database = new Database("vaderserver0.dhcp.asu.edu", 27017);
		database.getDatabase("scotlandstream48hour");
		database.getCollection("tweet");
		
		Database outputData = new Database("vaderserver0.dhcp.asu.edu", 27017);
		outputData.getDatabase("scotlandstream48hour");
		outputData.getCollection("hour");

		Calendar startTime = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ENGLISH);
		Calendar endTime = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ENGLISH);
		startTime.set(2014, 8, 16, 0, 6, 59);
		endTime.set(2014, 8, 16, 1, 0, 0);
		BasicDBObject query = new BasicDBObject("timestamp", new BasicDBObject(
					"$gte", startTime.getTimeInMillis()
				).append("$lt", endTime.getTimeInMillis()));
		
		DBCursor cursor = database.coll.find(query);
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString("text");
				BasicDBList entities = NLP.annotateDBObject(text);
				BasicDBObject outObj = mongoObj.append("ner", entities);
				outputData.coll.insert(outObj);
			}
		}
		finally{
			cursor.close();
		}
	}

}

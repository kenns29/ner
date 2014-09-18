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
		TimeZone timeZone = TimeZone.getTimeZone("UTC");
		Calendar calendar = new GregorianCalendar();
		Date startTime = null;
		Date endTime = null;
		try {
			startTime = new SimpleDateFormat("MM/dd/yy HH:mm:ss", Locale.ENGLISH).parse("09/16/2014 12:00:00");
			endTime = new SimpleDateFormat("MM/dd/yy HH:mm:ss", Locale.ENGLISH).parse("09/16/2014 13:00:00");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BasicDBObject query = new BasicDBObject("timestamp", new BasicDBObject(
					"$gte", startTime.getTime()
				).append("$lt", endTime.getTime()));
		
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

package timer;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;

import nerAndGeo.Database;
import nerAndGeo.Main;

public class ThreadProgressHandler {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private Database database = null;
	private DB db = null;
	private DBCollection coll = null;
	
	public ThreadProgressHandler(){
		try {
			database = new Database(Main.configPropertyValues.periodicThreadProgressHost, Main.configPropertyValues.periodicThreadProgressPort);
		} catch (UnknownHostException e) {
			HIGH_PRIORITY_LOGGER.error("did not successfully start the database for Thread Progress", e);
		}
		db = database.getDatabase(Main.configPropertyValues.periodicThreadProgressDatabase);
		coll = db.getCollection(Main.configPropertyValues.periodicDocumentProcessTimeCollection);
	}
	
	public void checkStatus(){
		
	}
}

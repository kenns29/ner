package nerAndGeo;

import java.io.FileNotFoundException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;

public class GeonameServiceChecker implements Runnable {
	private static Logger LOGGER = Logger.getLogger("reportsLog");
	//private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	
	private String name = "UK";
	private long waitTime = 300000;
	public GeonameServiceChecker(){}
	public GeonameServiceChecker(String name){
		this.name = name;
	}
	@Override
	public void run() {
		while(true){
			try {
				BasicDBObject rObj = Geoname.geocode(name);
				break;
			} 
			catch (SocketTimeoutException e){
				try {
					Thread.sleep(this.waitTime);
				} catch (InterruptedException e1) {
					LOGGER.error("Geoname Service not Available, Checking again in 5 min", e1);
				}
			}
			catch (ConnectException e){
				try {
					Thread.sleep(this.waitTime);
				} catch (InterruptedException e1) {
					LOGGER.error("Geoname Service not Available, Checking again in 5 min", e1);
				}
			}
			catch (FileNotFoundException e){
				try {
					Thread.sleep(this.waitTime);
				} catch (InterruptedException e1) {
					LOGGER.error("Geoname Service not Available, Checking again in 5 min", e1);
				}
			}
			catch (Exception e) {
				break;
			}
		}
		synchronized(Main.lockObjectGeonameServiceChecker){
			Main.geonameServiceAvailable = true;
		}
	}
	
}

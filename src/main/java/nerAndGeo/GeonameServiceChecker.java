package nerAndGeo;

import java.io.FileNotFoundException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

import com.mongodb.BasicDBObject;

public class GeonameServiceChecker implements Runnable {
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
					e1.printStackTrace();
				}
			}
			catch (ConnectException e){
				
			}
			catch (FileNotFoundException e){
				
			}
			catch (Exception e) {
				
				break;
			}
		}
	}
	
}

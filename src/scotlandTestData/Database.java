package scotlandTestData;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;


public class Database{
	public MongoClient mongoClient = null;
	public Database() throws UnknownHostException{
		mongoClient = new MongoClient();
	}
	public Database(String name) throws UnknownHostException{
		mongoClient = new MongoClient(name);
	}
	public Database(String name, int port) throws UnknownHostException{
		mongoClient = new MongoClient(name, port);
	}
	public DB getDatabase(String name){
		return mongoClient.getDB(name);
	}
	
	public void close(){
		mongoClient.close();
	}
}

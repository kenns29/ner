package nerAndGeo;

import java.util.ArrayList;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

public class NERThreadList {
	public static ArrayList<NERThread> list = new ArrayList<NERThread>();
	public static ArrayList<Thread> threadList = new ArrayList<Thread>();
	public static ArrayList<ArrayList<BasicDBObject>> threadDataList = new ArrayList<ArrayList<BasicDBObject>>();
	
	public static ObjectId getSafeObjectId(ArrayList<NERThread> nerThreadList){
		if(nerThreadList.size() > 1){
			ObjectId smallestObjectId = nerThreadList.get(0).threadStatus.currentObjectId;
			for(int i = 1; i < nerThreadList.size(); i++){
				if(smallestObjectId.compareTo(nerThreadList.get(i).threadStatus.currentObjectId) >= 1){
					smallestObjectId = nerThreadList.get(i).threadStatus.currentObjectId;
				}
			}
			return smallestObjectId;
		}
		else{
			return null;
		}
	}
}

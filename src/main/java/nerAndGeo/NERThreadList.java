package nerAndGeo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import org.bson.types.ObjectId;

public class NERThreadList {
	public static ArrayList<NERThread> list = new ArrayList<NERThread>();
	public static ArrayList<Thread> threadList = new ArrayList<Thread>();
	
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
	
	public static synchronized LinkedList<LinkedHashMap<String, Object>> nerThreadStatusToLinkedList(){
		LinkedList<LinkedHashMap<String, Object>> rL = new LinkedList<LinkedHashMap<String, Object>>();
		for(NERThread t : list){
			rL.add(t.threadStatus.toLinkedHashMap());
		}
		return rL;
	}
}

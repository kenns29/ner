package nerAndGeo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import org.bson.types.ObjectId;

public class NERThreadList {
	public static ArrayList<NERThread> list = new ArrayList<NERThread>();
	public static ArrayList<Thread> threadList = new ArrayList<Thread>();
	
	public static ObjectId getSafeObjectId(ArrayList<NERThread> nerThreadList){
		if(nerThreadList.size() > 0){
			synchronized(NER.class){
				ObjectId smallestObjectId = nerThreadList.get(0).threadStatus.currentObjectId;
				int i = 1;
				for(; i < nerThreadList.size() && smallestObjectId == null; i++){
					smallestObjectId = nerThreadList.get(i).threadStatus.currentObjectId;
				}
				for(int j = i; j < nerThreadList.size(); j++){
					ObjectId nextObjectId = nerThreadList.get(j).threadStatus.currentObjectId;
					if(nextObjectId != null && smallestObjectId.compareTo(nextObjectId) >= 1){
						smallestObjectId = nextObjectId;
					}
				}
				
				return smallestObjectId;
			}
		}
		else{
			return null;
		}
	}
	
	public static LinkedList<LinkedHashMap<String, Object>> nerThreadStatusToLinkedList(){
		LinkedList<LinkedHashMap<String, Object>> rL = new LinkedList<LinkedHashMap<String, Object>>();
		synchronized(NER.class){
			for(NERThread t : list){
				rL.add(t.threadStatus.toLinkedHashMap());
			}
		}
		return rL;
	}
}

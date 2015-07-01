package util;

import org.bson.types.ObjectId;

public class ThreadStatus {
	public int threadId = 0;
	public ObjectId currentObjectId = null;
	public long currentInsertionTime = 0;
	public long currentTweetId = 0;
	public TimeRange timeRange = null;
	
	public ThreadStatus(int threadId){
		this.threadId = threadId;
	}
}

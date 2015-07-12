package util;

public class TaskType {
	public static final int NORMAL_TASK = 0;
	public static final int RETRY_TASK = 1;
	private int type = 0;
	public TaskType(){}
	public TaskType(int type){
		setType(type);
	}
	public void setType(int type){
		this.type = type;
	}
	public int getType(){
		return this.type;
	}
	public String toString(){
		switch(type){
		case NORMAL_TASK:
			return "NORMAL TASK";
		case RETRY_TASK:
			return "RETRY TASK";
		default:
			return "NORMAL TASK";
		}
	}
	
}

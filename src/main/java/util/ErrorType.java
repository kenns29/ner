package util;

public class ErrorType {
	public static final int OTHER = 0;
	public static final int SOCKET_TIME_OUT = 1;
	
	private int type = 0;
	
	public ErrorType(){}
	
	public ErrorType(int type){
		setType(type);
	}
	public void setType(int type){
		this.type = type;
	}
	
	public int getType(){
		return this.type;
	}
	
	public String toString(){
		switch(this.type){
		case OTHER:
			return "Other";
		case SOCKET_TIME_OUT:
			return "SocketTimeoutException";
		default:
			return "Other";
		}
	}
	
}
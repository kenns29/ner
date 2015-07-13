package util;

public class ErrorStatus {
	public ErrorType errorType = new ErrorType();
	private int errorCount = 0;
	public String errorMsg = null;
	public ErrorStatus(){}
	public ErrorStatus(ErrorType errorType){
		this.errorType = errorType;
	}
	public ErrorStatus(ErrorType errorType, int errorCount){
		this(errorType);
		this.errorCount = errorCount;
	}
	public ErrorStatus(ErrorType errorType, int errorCount, String errorMsg){
		this(errorType, errorCount);
		this.errorMsg = errorMsg;
	}
	public void incErrorCount(){
		++this.errorCount;
	}
	
	public void decErrorCount(){
		--this.errorCount;
	}
	public void setErrorCount(int errorCount){
		this.errorCount = errorCount;
	}
	public void zeroErrorCount(){
		this.setErrorCount(0);
	}
	public int getErrorCount(){
		return this.errorCount;
	}
}

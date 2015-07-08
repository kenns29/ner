package version;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class VersionControl {
	private String versionNumber = null;
	private DateTime date = null;
	private String title = "NERExtractor";
	private static final int LINE_LENGTH = 50;
	public VersionControl(String versionNumber, DateTime date){
		this.versionNumber = versionNumber;
		this.date = date;
	}
	
	public VersionControl(String versionNumber, String dateString){
		DateTimeFormatter formatter = DateTimeFormat.forPattern("MM-dd-yyyy").withZone(DateTimeZone.UTC);
		DateTime date = formatter.parseDateTime(dateString);
		this.versionNumber = versionNumber;
		this.date = date;
		
	}
	
	public String toLog(){
		String rStr = "";
		
		for(int i = 0; i < LINE_LENGTH; i++){
			rStr += "#";
		}
		rStr += "\n";
		rStr += outputLine(this.title) + "\n"
			  + outputLine("version: " + this.versionNumber) + "\n"
			  + outputLine("Date: " + this.date.toString("MM-dd-yyyy")) + "\n";
	    
		for(int i = 0; i < LINE_LENGTH; i++){
			rStr += "#";
		}
		rStr += "\n";
		return rStr;
	}
	
	private String outputLine(String str){
		int sideLength = (LINE_LENGTH - str.length()) / 2;
		int leftLength = 0;
		int rightLength = 0;
		if((LINE_LENGTH - str.length()) % 2 == 0){
			leftLength = rightLength = sideLength;
		}
		else{
			leftLength = sideLength;
			rightLength = sideLength + 1;
		}
		
		//add left line
		String leftLine = "#";
		for(int i = 0; i < leftLength - 1; i++){
			leftLine += " ";
		}
		//add right line
		String rightLine = "";
		for(int i = 0; i < rightLength -1; i++){
			rightLine += " ";
		}
		rightLine += "#";
		return leftLine + str + rightLine;
	}
}

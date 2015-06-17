package nerAndGeo;

import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;

public class LoggerAttr {
	public static Handler consoleHandler = new ConsoleHandler();
	public static Handler fileHandler = null;
	static{
		try{
			fileHandler = new FileHandler("log.log");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}

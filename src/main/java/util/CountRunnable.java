package util;

import nerAndGeo.Main;

public class CountRunnable implements Runnable{
	@Override
	public void run() {
		Main.totalDocuments = CollUtilities.getTotalDocumentCountWithStopAtEnd(Main.mainColl);
	}
}

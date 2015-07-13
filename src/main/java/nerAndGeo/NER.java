package nerAndGeo;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.simple.JSONArray;

import simpleHttpServer.StatusHttpServer;
import util.CollUtilities;
import util.ThreadStatus;
import util.TimeRange;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NormalizedNamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;



public class NER {
	private static AtomicInteger pipelineErrCount = new AtomicInteger(0);
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static final Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static JSONArray performAnnotation(String text) throws IOException{
		
		Properties NLPprops = new Properties();
		NLPprops.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(NLPprops);
		Annotation document = new Annotation(text);
		pipeline.annotate(document);
		
	
		JSONArray entities = new JSONArray();
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		
		for(CoreMap sentence: sentences){
			String previousWord, previousNe = null;
			
			for (CoreLabel token: sentence.get(TokensAnnotation.class)){
				String word = token.getString(TextAnnotation.class);
				String pos = token.getString(PartOfSpeechAnnotation.class);
				String ne = token.getString(NamedEntityTagAnnotation.class);
				String nne = token.getString(NormalizedNamedEntityTagAnnotation.class);
				LinkedHashMap obj = new LinkedHashMap();
				if(!ne.equals("O")){
					obj.put("entity", word);
					obj.put("namedEntity", ne);
					entities.add(obj);
				}
			}
		}
		
		return entities;
	}
	
	public static BasicDBList annotateDBObject(String text){
		Properties NLPprops = new Properties();
		NLPprops.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(NLPprops);
		return annotateDBObject(text, pipeline, new TimeRange(0, 0));
	}
	public static BasicDBList annotateDBObject(String text, StanfordCoreNLP pipeline){
		return annotateDBObject(text, pipeline, new TimeRange(0, 0));
	}
	
	public static BasicDBList annotateDBObject(String text, StanfordCoreNLP pipeline, TimeRange timeRange){
		Annotation document = new Annotation(text);
		
		boolean annotationSuccess = false;
		boolean isSecondTry = false;
		do{
			try{
				pipeline.annotate(document);
				annotationSuccess = true;
				if(isSecondTry){
					LOGGER.info("Succeed the annotation after the first attempt. for Thread from " + timeRange.toString());
				}
			}
			catch(Exception e){
				annotationSuccess = false;
				synchronized(NER.class){
					int pCount = pipelineErrCount.intValue();
					if(!isSecondTry){
						pCount = pipelineErrCount.incrementAndGet();
					}
					isSecondTry = true;
					LOGGER.error("Pipline Annotation Error, text: " + text 
							+ "\nIn Thread from " + timeRange.toString()
							+ "\nThere are total of " + pCount + " such errors", e);
				}
				//e.printStackTrace();
			}
		} while(!annotationSuccess);
		BasicDBList mongoList = new BasicDBList();
		
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		
		for(CoreMap sentence: sentences){
			String finalWord = "", previousNe = "";
			BasicDBObject mongoObj = null;
			for (CoreLabel token: sentence.get(TokensAnnotation.class)){
				String word = token.getString(TextAnnotation.class);
				//String pos = token.getString(PartOfSpeechAnnotation.class);
				String ne = token.getString(NamedEntityTagAnnotation.class);
				//String nne = token.getString(NormalizedNamedEntityTagAnnotation.class);
				
				if(!ne.equals("O")){
					if(!ne.equals(previousNe)){
						
						if(!finalWord.equals("") && !previousNe.equals("")){
							mongoObj = new BasicDBObject("entity", finalWord)
								.append("entityType", previousNe);
							mongoList.add(mongoObj);
						}
						previousNe = ne;
						finalWord = word;
					}
					else{
						if(finalWord.equals(""))
							finalWord = word;
						else{
							String finalCandidate = finalWord + " " + word;
							if(!text.contains(finalCandidate)){
								mongoObj = new BasicDBObject("entity", finalWord)
									.append("entityType", previousNe);
								mongoList.add(mongoObj);
								finalWord = word;
							} else {
								finalWord = finalCandidate;
							}
						}
						previousNe = ne;
					}
					
				}
				//System.out.println("word = " + word + "\tpos = " + pos + "\tne = " + ne + "\tnne = " + nne);
			}
			if(!previousNe.equals("") && !finalWord.equals("")){
				mongoObj = new BasicDBObject("entity", finalWord)
					.append("entityType", previousNe);
				mongoList.add(mongoObj);
			}
		}
		return mongoList;
	}

	public static BasicDBList annotateDBObject(String text, int length){
		BasicDBList result = null;
		return result;
	}
	
	//////////////////////////////
	/////Parallel NER Driver//////
	//////////////////////////////
	public static void parallelNER(DBCollection coll, String inputField, ObjectId pStartObjectId, ObjectId pEndObjectId, BlockingQueue<TimeRange> queue){
		ObjectId startObjectId = null;
		ObjectId endObjectId = null;
		if(pStartObjectId == null){
			startObjectId = CollUtilities.minObjectId(coll);
		}
		else{
			startObjectId = pStartObjectId;
		}
		if(pEndObjectId == null){
			endObjectId = CollUtilities.maxObjectId(coll);
		}
		else{
			endObjectId = pEndObjectId;
		}
		
		NERTaskManager nerTaskManager = new NERTaskManager(startObjectId, endObjectId, queue, coll);
		for(int i = 0; i < Main.configPropertyValues.core; i++){
			NERThreadList.list.add(new NERThread(coll, inputField, queue, new ThreadStatus(i)));
		}
		
		for(int i = 0; i < Main.configPropertyValues.core; i++){
			NERThreadList.threadList.add(new Thread(NERThreadList.list.get(i)));
		}
		
		new Thread(nerTaskManager).start();
		for(int i = 0; i < Main.configPropertyValues.core; i++){
			NERThreadList.threadList.get(i).start();
		}
		
		try {
			@SuppressWarnings("unused")
			StatusHttpServer statusHttpServer = new StatusHttpServer();
		} catch (IOException e) {
			HIGH_PRIORITY_LOGGER.fatal("Did not successfully start the server", e);;
		}
		
		checkStatus();
	}
	
	public static void parallelNER(DBCollection coll, String inputField, long pMinTime, long pMaxTime, BlockingQueue<TimeRange> queue){
		long minTime = -1;
		long maxTime = -1;
		if(pMinTime < 0){
			if(Main.configPropertyValues.useInsertionOrCreationTime == 0){
				minTime = CollUtilities.minInsertionTime(coll);
			}
			else{
				minTime = CollUtilities.minTime(coll);
			}
		}
		else{
			minTime = pMinTime;
		}
		
		if(pMaxTime < 0){
			if(Main.configPropertyValues.useInsertionOrCreationTime == 0){
				maxTime = CollUtilities.maxInsertionTime(coll);
			}
			else{
				maxTime = CollUtilities.maxTime(coll);
			}
		}
		else{
			maxTime = pMaxTime;
		}
		
		NERTaskManager nerTaskManager = new NERTaskManager(minTime, maxTime, queue, coll);
		for(int i = 0; i < Main.configPropertyValues.core; i++){
			NERThreadList.list.add(new NERThread(coll, inputField, queue, new ThreadStatus(i)));
		}
		
		for(int i = 0; i < Main.configPropertyValues.core; i++){
			NERThreadList.threadList.add(new Thread(NERThreadList.list.get(i)));
		}
		
		new Thread(nerTaskManager).start();
		for(int i = 0; i < Main.configPropertyValues.core; i++){
			NERThreadList.threadList.get(i).start();
		}
		
		try {
			@SuppressWarnings("unused")
			StatusHttpServer statusHttpServer = new StatusHttpServer();
		} catch (IOException e) {
			HIGH_PRIORITY_LOGGER.fatal("Did not successfully start the server", e);;
		}
		
		checkStatus();
	}
	
	public static void parallelNER(DBCollection coll, String inputField, BlockingQueue<TimeRange> queue){
		// 0: split simply by time intervals
		// 1: split by the number of documents
		if(Main.configPropertyValues.splitOption == 0){
			long minTime = CollUtilities.minInsertionTime(coll);
			long maxTime = CollUtilities.maxInsertionTime(coll);
			parallelNER(coll, inputField, minTime, maxTime, queue);
		}
		else{
			ObjectId minObjectId = CollUtilities.minObjectId(coll);
			ObjectId maxObjectId = CollUtilities.maxObjectId(coll);
			parallelNER(coll, inputField, minObjectId, maxObjectId, queue);
		}
	}
	
	private static void checkStatus(){
		while(true){
			try {
				Thread.sleep(60000);
				synchronized(NER.class){
					long time = System.currentTimeMillis();
					String msg = "";
					for(int i = 0; i < Main.configPropertyValues.core; i++){
						msg += NERThreadList.list.get(i).threadStatus.toString() + "\n";
					}
					ObjectId safeObjectId = NERThreadList.getSafeObjectId(NERThreadList.list);
					LOGGER.info(msg
							+ "\nFrom " + new TimeRange(Main.mainPreTime, time).toString() + ", " + Main.timelyDocCount.intValue() + " are processed. The time range is " + (time - Main.mainPreTime) + " milliseconds."
							+ "\nThe Safe Object Id is " + safeObjectId);
					Main.lastTimelyDocCount.set(Main.timelyDocCount.intValue());
					Main.timelyDocCount.set(0);
					Main.mainPreTime = time;
					
					for(int i = 0; i < Main.configPropertyValues.core; i++){
						Thread t = NERThreadList.threadList.get(i);
						if(!t.isAlive()){
							ThreadStatus tStatus = NERThreadList.list.get(i).threadStatus;
							String msg1 = "";
							msg1 += tStatus.toThreadIdString() + " is inactive."
									+ "\nCurrent Thread Status: " + tStatus.toString();
							LOGGER.error(msg1);
							
							String msg2 = "Time Range " + tStatus.timeRange.toString() + " has not been fully processed."
									+ "\nPossible document that causes the error is " + tStatus.currentObjectId + "."
									+ "\nCurrent Thread Status is " + tStatus.toString() + ".";
						
							HIGH_PRIORITY_LOGGER.fatal(msg2);
							NERThreadList.threadList.set(i, new Thread(NERThreadList.list.get(i)));
							NERThreadList.threadList.get(i).start();
						}
					}
				}
			} catch (InterruptedException e) {
				HIGH_PRIORITY_LOGGER.fatal("Main Thread Interrupted.", e);
			} catch (Exception e){
				HIGH_PRIORITY_LOGGER.fatal("Main Thread Unexpected Exception", e);
			}
		}
	}
	////////////////////////////////
	//// ///////Other///////////////
	////////////////////////////////
	public static BasicDBList insertFromFlag(BasicDBList ner, String flag){
		if(flag != null){
			BasicDBList outList = new BasicDBList();
			for(Object e : ner){
				BasicDBObject nerObj = (BasicDBObject)e;
				nerObj.put("from", flag);
				outList.add(nerObj);
			}
			return outList;
		}
		else{
			return ner;
		}
	}
	
	/////////////////////////////
	//use for single thread NER//
	/////////////////////////////
	public static void insertNer(DBCollection coll, String inputField){
		BasicDBObject query = new BasicDBObject("ner", null);
		DBCursor cursor = coll.find();
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("There are total of " + cursor.count() + "items in the query");
		int count = 0;
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				if(text != null && text.length() < 1000){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					BasicDBList entities = NER.annotateDBObject(text);
					
					System.out.println(entities.toString());
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
										new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)));
					
					++count;
				}
				
				if(count % 100 == 0){
					System.out.println(count + " updated");
				}
			}
		}
		finally{
			cursor.close();
		}
	}
	
	public static void insertNer(DBCollection coll, String inputField, BasicDBObject query){
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("Finished Querying, there are total of " + cursor.count() + "items");
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				String text = mongoObj.getString(inputField);
				if(text != null && text.length() < 1000){
					text = text.replaceAll("http:/[/\\S+]+|@|#|", "");
					BasicDBList entities = NER.annotateDBObject(text);
					
					coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
										new BasicDBObject("$set", new BasicDBObject(Main.configPropertyValues.nerOutputField, entities)));
				}
			}
		}
		finally{
			cursor.close();
		}
	}
}
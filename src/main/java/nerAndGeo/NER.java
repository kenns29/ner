package nerAndGeo;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.json.simple.JSONArray;

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
	public static int textEntitiesDocCount = 0;
	public static int userEntitiesDocCount = 0;
	private static int pipelineErrCount = 0;
	private static final Logger LOGGER = Logger.getLogger(NER.class.getName());
	static{
		LOGGER.addHandler(LoggerAttr.fileHandler);
	}
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
				//System.out.println("word = " + word + "\tpos = " + pos + "\tne = " + ne + "\tnne = " + nne);
			}
		}
		
		return entities;
	}
	
	public static BasicDBList annotateDBObject(String text){
		Properties NLPprops = new Properties();
		NLPprops.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(NLPprops);
		return annotateDBObject(text, pipeline, "", "");
	}
	public static BasicDBList annotateDBObject(String text, StanfordCoreNLP pipeline){
		return annotateDBObject(text, pipeline, "", "");
	}
	
	public static BasicDBList annotateDBObject(String text, StanfordCoreNLP pipeline, String startTimeStr, String endTimeStr){
		Annotation document = new Annotation(text);
		
		boolean annotationSuccess = false;
		boolean isSecondTry = false;
		do{
			try{
				pipeline.annotate(document);
				annotationSuccess = true;
				if(isSecondTry){
					LOGGER.info("Succeed the annotation after the first attempt. for Thread from (startTime " + startTimeStr + " to endTime " + endTimeStr + ")");
				}
			}
			catch(Exception e){
				annotationSuccess = false;
				if(!isSecondTry){
					++NER.pipelineErrCount;
				}
				isSecondTry = true;
				LOGGER.severe("Pipline Annotation Error, text: " + text + "\nIn Thread from (startTime " + startTimeStr + " to endTime " + endTimeStr + ")\n"
						+ "There are total of " + NER.pipelineErrCount + " such errors");
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
	
	public static void parallelNer(DBCollection coll, String inputField){
		long minTime = CollUtilities.minTime(coll);
		long maxTime = CollUtilities.maxTime(coll);
		parallelNer(coll, inputField, minTime, maxTime);
	}
	
	public static void parallelNer(DBCollection coll, String inputField, long minTime, long maxTime){
		NERThreadPool nerThreadPool = new NERThreadPool(coll, inputField, Main.configPropertyValues.core, minTime, maxTime);
		nerThreadPool.run();
	}
	
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
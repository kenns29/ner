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
	private static final Logger LOGGER = Logger.getLogger(NER.class.getName());
	static{
		LOGGER.addHandler(LoggerAttr.fileHandler);
	}
	public static JSONArray performAnnotation(String text) throws IOException{
			
		Annotation document = new Annotation(text);
		Main.pipeline.annotate(document);
		
	
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
					obj.put("mentionSpan", word);
					obj.put("namedEntity", ne);
					entities.add(obj);
				}
				//System.out.println("word = " + word + "\tpos = " + pos + "\tne = " + ne + "\tnne = " + nne);
			}
		}
		
		return entities;
	}
	
	public static BasicDBList annotateDBObject(String text){
		//StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		return annotateDBObject(text, Main.pipeline);
	}
	
	public static BasicDBList annotateDBObject(String text, StanfordCoreNLP pipeline){
		Annotation document = new Annotation(text);
		try{
			pipeline.annotate(document);
		}
		catch(Exception e){
			LOGGER.severe("Pipline Annotation Error, text: " + text);
			e.printStackTrace();
		}
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
							mongoObj = new BasicDBObject("mentionSpan", finalWord)
								.append("namedEntity", previousNe);
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
								mongoObj = new BasicDBObject("mentionSpan", finalWord)
									.append("namedEntity", previousNe);
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
				mongoObj = new BasicDBObject("mentionSpan", finalWord)
					.append("namedEntity", previousNe);
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
	
	public static void insertNer(DBCollection coll, String inputField){
		BasicDBObject query = new BasicDBObject("ner1", null);
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
										new BasicDBObject("$set", new BasicDBObject("ner1", entities)));
					
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
										new BasicDBObject("$set", new BasicDBObject("ner", entities)));
				}
			}
		}
		finally{
			cursor.close();
		}
	}
}
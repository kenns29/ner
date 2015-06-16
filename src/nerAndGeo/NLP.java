package nerAndGeo;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import org.json.simple.JSONArray;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

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



public class NLP {
	
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
		
		
		Annotation document = new Annotation(text);
		try{
			Main.pipeline.annotate(document);
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println(text);
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
}
package nerAndGeo;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class GeoCoding {
	public static LinkedHashMap getCoord(String location){
		URL url;
		String urlParameter;
		HttpURLConnection connection = null;
		LinkedHashMap coord = null;
		
		JSONParser parser = new JSONParser();
		ContainerFactory containerFactory = new ContainerFactory(){
			public List creatArrayContainer(){
				return new JSONArray();
			}
			public Map createObjectContainer(){
				return new LinkedHashMap();
			}

		};
		
		//GeoCoding
		try{
			
			urlParameter = "sensor=false&address="+location.replaceAll(" ","+");
			url = new URL("http://vaderserver0.cidse.dhcp.asu.edu:8080/maps/api/geocode/json?" + urlParameter);
			//url = new URL("http://www.datasciencetoolkit.org/maps/api/geocode/json?" + urlParameter);
			//System.out.println(urlParameter);
			connection = (HttpURLConnection)url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Content-Type", "application/json");
			//connection.setRequestProperty("Content-Length", Integer.toString(urlParameter.getBytes().length));
			connection.setRequestProperty("Content-language", "en-US");
			
			/*DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			
			wr.writeBytes(urlParameter);
			wr.flush();
			wr.close();*/
			
			InputStream is = connection.getInputStream();
			
			BufferedReader rd = new BufferedReader(new InputStreamReader(is));
			String line;
			StringBuffer response = new StringBuffer();
			while(	(line = rd.readLine()) != null){
				response.append(line);
				response.append('\r');
			}
			rd.close();
			
			//System.out.println(response.toString());
			
			
			try{
				Map json = (Map) parser.parse(response.toString(), containerFactory);
				
				JSONArray results = (JSONArray)json.get("results");
				
				if(results.size() > 0){
					coord = (LinkedHashMap)((LinkedHashMap)((LinkedHashMap)results.get(0)).get("geometry")).get("location");
				}
				//System.out.println(coord.toString());
				
			}
			catch(ParseException pe){
				System.out.println(pe);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			if(connection != null){
				connection.disconnect();
			}
		}
		return coord;
	}
	
	public static String getCountry(LinkedHashMap coord){
		String result = null;
		URL url;
		String urlParameter;
		HttpURLConnection connection = null;
		
		JSONParser parser = new JSONParser();
		ContainerFactory containerFactory = new ContainerFactory(){
			public List creatArrayContainer(){
				return new JSONArray();
			}
			public Map createObjectContainer(){
				return new LinkedHashMap();
			}

		};
		
		//Coordinate to Politics
		if(coord != null){
			try{
				double lat = (double)coord.get("lat");
				double lng = (double)coord.get("lng");
				urlParameter = lat + "%2c" + lng;
				url = new URL("http://vaderserver0.cidse.dhcp.asu.edu:8080/coordinates2politics/" + urlParameter);
				//url = new URL("http://www.datasciencetoolkit.org/coordinates2politics/" + urlParameter);
				connection = (HttpURLConnection)url.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("Content-Type", "application/json");
				connection.setRequestProperty("Content-language", "en-US");
				
				InputStream is = connection.getInputStream();
				
				BufferedReader rd = new BufferedReader(new InputStreamReader(is));
				String line;
				StringBuffer response = new StringBuffer();
				while(	(line = rd.readLine()) != null){
					response.append(line);
					response.append('\r');
				}
				rd.close();
				
				try{
					JSONArray jsonArray = (JSONArray) parser.parse(response.toString(), containerFactory);
					
					JSONArray politics = (JSONArray)((LinkedHashMap)jsonArray.get(0)).get("politics");
					result = (String)((LinkedHashMap)politics.get(0)).get("name");
					//System.out.println(result);
					
				} 
				catch(ParseException pe){
					System.out.println(pe);
				}
				
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				if(connection != null){
					connection.disconnect();
				}
			}
		}
		
		return result;
	}
	
	public static String getCountry(String location){
		LinkedHashMap coord = getCoord(location);
		String result = getCountry(coord);
		
		return result;
	}
	
	public static void insertLocation(DBCollection coll){
		BasicDBObject query = new BasicDBObject("geocoding", null)
		.append("ner", new BasicDBObject("$ne", null));
		
		insertLocation(coll, query);
	}
	
	public static void insertLocation(DBCollection coll, BasicDBObject query){
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("Finished query, there are total of " + cursor.count() + " items.");
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				BasicDBList ner = (BasicDBList) mongoObj.get("ner");
				BasicDBList locArray = new BasicDBList();
				BasicDBList outList = new BasicDBList();
				for(Object e: ner){
					BasicDBObject entity = (BasicDBObject)e;
					if(entity.getString("entityType").equals("LOCATION")){
						String inputLoc = entity.getString("entity");
						if(!(	inputLoc.equals("Africa")
								|| 	inputLoc.equals("Europe")
								|| 	inputLoc.equals("Asia")
								||	inputLoc.equals("North America")
								|| 	inputLoc.equals("South America")
								|| 	inputLoc.equals("Antarctica")	)){
							LinkedHashMap coord = GeoCoding.getCoord(inputLoc);
							String loc = GeoCoding.getCountry(coord);
							
							if(!locArray.contains(loc) && loc != null){
								outList.add(new BasicDBObject("name", loc)
								.append("coord", new BasicDBObject("lat", (double)coord.get("lat"))
													.append("lng", (double)coord.get("lng")))
								.append("type", "country"));
								locArray.add(loc);
							}
						}
					}
				}
				
				coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")), 
						new BasicDBObject("$set", new BasicDBObject("geocoding", outList)));
				
			}
		}
		finally{
			cursor.close();
		}
	}
	
	public static void insertCoord(DBCollection coll){
		BasicDBObject query = new BasicDBObject("geoCoord", null)
		.append("ner", new BasicDBObject("$ne", null));
		
		DBCursor cursor = coll.find(query);
		cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
		try{
			while(cursor.hasNext()){
				BasicDBObject mongoObj = (BasicDBObject) cursor.next();
				BasicDBList ner = (BasicDBList) mongoObj.get("ner");
				BasicDBList outList = new BasicDBList();
				for(Object e : ner){
					BasicDBObject entity = (BasicDBObject) e;
					if(entity.getString("entityType").equals("LOCATION")){
						String inputLoc = entity.getString("entity");
						if(!(	inputLoc.equals("Africa")
								|| 	inputLoc.equals("Europe")
								|| 	inputLoc.equals("Asia")
								||	inputLoc.equals("North America")
								|| 	inputLoc.equals("South America")
								|| 	inputLoc.equals("Antarctica")	)){
							LinkedHashMap coord = GeoCoding.getCoord(inputLoc);
							String loc = GeoCoding.getCountry(coord);
							if(coord != null && loc != null){
								outList.add(new BasicDBObject("name", inputLoc)
									.append("country", loc)
									.append("coord", new BasicDBObject("lat", (double)coord.get("lat"))
													.append("lng", (double)coord.get("lng"))));
							}
						}
					}
				}
				coll.update(new BasicDBObject("_id", mongoObj.getObjectId("_id")),
						new BasicDBObject("$set", new BasicDBObject("geoCoord", outList)));
			}
		}
		finally{
			cursor.close();
		}
	}
}

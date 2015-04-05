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
}

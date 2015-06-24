package nerAndGeo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class GeojsonList {
	public BasicDBList geometryList = new BasicDBList();
	public BasicDBObject geometryCollection = new BasicDBObject();
	public GeojsonList(){
		geometryCollection.append("type", "GeometryCollection");
		geometryCollection.append("geometries", geometryList);
	}
	
	public void updateCollectionGeometries(){
		geometryCollection.put("geometries", geometryList);
	}
	
	public void addFromGeoname(BasicDBObject geonameObj){
		BasicDBObject geojsonObj = makeGeojsonObjFromGeoname(geonameObj);
		if(geojsonObj != null)
			geometryList.add(geojsonObj);
	}
	
	public void addFromNerGeonameList(BasicDBList nerGeonameList){
		for(Object e : nerGeonameList){
			BasicDBObject nerGeonameObj = (BasicDBObject) e;
			BasicDBObject geonameObj = (BasicDBObject) nerGeonameObj.get(Main.configPropertyValues.geonameOutputField);
			if(geonameObj != null){
				addFromGeoname(geonameObj);
			}
		}
		updateCollectionGeometries();
	}
	
	public void addFromGeonameList(BasicDBList geonameList){
		for(Object e : geonameList){
			BasicDBObject geonameObj = (BasicDBObject) e;
			addFromGeoname(geonameObj);
		}
		updateCollectionGeometries();
	}
	
	public BasicDBObject makeGeojsonObjFromGeoname(BasicDBObject geonameObj){
		BasicDBObject rObj = null;
		BasicDBObject coord = (BasicDBObject) geonameObj.get("coord");
		if(coord != null){
			double lat = coord.getDouble("lat");
			double lng = coord.getDouble("lng");
			
			BasicDBList coordinate = new BasicDBList();
			coordinate.add(lat);
			coordinate.add(lng);
			rObj = new BasicDBObject("type", "Point")
						.append("coordinates", coordinate);
		}
		return rObj;
	}
	
	public void addFromMongoCoord(BasicDBObject coordinates, BasicDBObject place, BasicDBObject location){
		if(coordinates == null){
			if(location != null && validLocation(location)){
				geometryList.add(makeGeojsonFromLocation(location));
			}
			else if(place != null){
				geometryList.add(makeGeojsonFromPlace(place));
			}
		}
		else{
			geometryList.add(makeGeojsonObjFromCoordinates(coordinates));
			if(location != null && validLocation(location)){
				if(compareCoordinatesWithLocation(coordinates, location)){
					if(place != null){
						geometryList.add(makeGeojsonFromPlace(place));
					}
				}
			}
			else if(place != null){
				geometryList.add(makeGeojsonFromPlace(place));
			}
		}
		
		updateCollectionGeometries();
	}
	
	public BasicDBObject makeGeojsonObjFromCoordinates(BasicDBObject coordinates){
		String type = coordinates.getString("type");
		BasicDBList point = (BasicDBList) coordinates.get("coordinates");
		double lat = (double) point.get(1);
		double lng = (double) point.get(0);
		BasicDBList newPoint = new BasicDBList();
		newPoint.add(lat);
		newPoint.add(lng);
		return new BasicDBObject("type", type)
					.append("coordinates", newPoint);
	}
	
	public BasicDBObject makeGeojsonFromLocation(BasicDBObject location){
		double lat = location.getDouble("lat");
		double lng = location.getDouble("lng");
		BasicDBList coordinates = new BasicDBList();
		coordinates.add(lat);
		coordinates.add(lng);
		return new BasicDBObject("type", "Point")
				.append("coordinates", coordinates);
	}
	
	public BasicDBObject makeGeojsonFromPlace(BasicDBObject place){
		BasicDBObject rObj = null;
		BasicDBObject bounding_box = (BasicDBObject) place.get("bounding_box");
		if(bounding_box != null){
			BasicDBList coordinates = (BasicDBList) bounding_box.get("coordinates");
			BasicDBList polygon = (BasicDBList) coordinates.get(0);
			BasicDBList firstPoint = (BasicDBList) polygon.get(0);
			BasicDBList lastPoint = (BasicDBList) polygon.get(polygon.size() - 1);
			
			if(!comparePoints(firstPoint, lastPoint)){
				polygon.add(firstPoint);
				coordinates = new BasicDBList();
				coordinates.add(polygon);
			}
			
			rObj = new BasicDBObject("type", "Polygon")
					.append("coordinates", coordinates);
		}
		return rObj;
	}
	
	
	
	public boolean isEmpty(){
		if(geometryList.size() > 0){
			return false;
		}
		else{
			return true;
		}
	}
	
	public static boolean comparePoints(BasicDBList firstPoint, BasicDBList secondPoint){
		double firstLat = (double) firstPoint.get(0);
		double firstLng = (double) firstPoint.get(1);
		double secondLat = (double) secondPoint.get(0);
		double secondLng = (double) secondPoint.get(1);
		
		if(firstLat == secondLat && firstLng == secondLng){
			return true;
		}
		else{
			return false;
		}
	}
	
	private boolean compareCoordinatesWithLocation(BasicDBObject coordinates, BasicDBObject location){
		BasicDBList coordPoint = (BasicDBList) coordinates.get("coordinates");
		double coordLat = (double) coordPoint.get(1);
		double coordLng = (double) coordPoint.get(0);
		
		double locationLat = location.getDouble("lat");
		double locationLng = location.getDouble("lng");
		
		if(coordLat == locationLat && coordLng == locationLng){
			return true;
		}
		else{
			return false;
		}
	}
	
	private boolean validLocation(BasicDBObject location){
		double lat = location.getDouble("lat");
		double lng = location.getDouble("lng");
		if(lat == 0 && lng == 0){
			return false;
		}
		else{
			return true;
		}
	}
}

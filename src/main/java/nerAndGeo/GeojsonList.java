package nerAndGeo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class GeojsonList {
	private static final double INVALID_DOUBLE = -1000.0;
	
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
			coordinate.add(lng);
			coordinate.add(lat);
			rObj = new BasicDBObject("type", "Point")
						.append("coordinates", coordinate);
		}
		return rObj;
	}
	
	public void addFromMongoCoord(BasicDBObject coordinates, BasicDBObject place, BasicDBObject location){
		if(coordinates != null){
			BasicDBObject newCoordinates = makeGeojsonObjFromCoordinates(coordinates);
			if(newCoordinates != null){
				geometryList.add(newCoordinates);
			}
		}
		if(place != null){
			geometryList.add(makeGeojsonFromPlace(place));
		}
		
		if(coordinates == null && place == null){
			if(location != null && validLocation(location)){
				geometryList.add(makeGeojsonFromLocation(location));
			}
		}
	}
	public BasicDBObject makeGeojsonObjFromCoordinates(BasicDBObject coordinates){
		String type = coordinates.getString("type");
		BasicDBList point = (BasicDBList) coordinates.get("coordinates");
		double lng = getDoubleFromCoordinatesItem(point.get(0));
		double lat = getDoubleFromCoordinatesItem(point.get(1));
		
		if(lat != INVALID_DOUBLE && lng != INVALID_DOUBLE){
			BasicDBList newPoint = new BasicDBList();
			newPoint.add(lng);
			newPoint.add(lat);
			return new BasicDBObject("type", type)
						.append("coordinates", newPoint);
		}
		else{
			return null;
		}
	}
	
	public BasicDBObject makeGeojsonFromLocation(BasicDBObject location){
		double lat = location.getDouble("lat");
		double lng = location.getDouble("lng");
		BasicDBList coordinates = new BasicDBList();
		coordinates.add(lng);
		coordinates.add(lat);
		return new BasicDBObject("type", "Point")
				.append("coordinates", coordinates);
	}
	
	public BasicDBObject makeGeojsonFromPlace(BasicDBObject place){
		BasicDBObject rObj = null;
		BasicDBObject bounding_box = (BasicDBObject) place.get("bounding_box");
		if(bounding_box != null){
			BasicDBList coordinates = (BasicDBList) bounding_box.get("coordinates");
			BasicDBList polygon = (BasicDBList) coordinates.get(0);
			for(int i = 0; i < polygon.size(); i++){
				BasicDBList point1 = (BasicDBList) polygon.get(i);
				for(int j = i+1; j < polygon.size(); j++){
					BasicDBList point2 = (BasicDBList) polygon.get(j);
					if(comparePoints(point1, point2) && !(i == 0 && j == (polygon.size() - 1))){
						BasicDBList rPoint = getMiddleOfPolygon(polygon);
						rObj = new BasicDBObject("type", "Point")
							.append("coordinates", rPoint);
						return rObj;
					}
				}
			}
			
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
	
	private boolean comparePoints(BasicDBList firstPoint, BasicDBList secondPoint){
		double firstLng = getDoubleFromCoordinatesItem(firstPoint.get(0));
		double firstLat = getDoubleFromCoordinatesItem(firstPoint.get(1));
		double secondLng = getDoubleFromCoordinatesItem(secondPoint.get(0));
		double secondLat = getDoubleFromCoordinatesItem(secondPoint.get(1));
		
		if(firstLat == secondLat && firstLng == secondLng){
			return true;
		}
		else{
			return false;
		}
	}
	
	private boolean compareCoordinatesWithLocation(BasicDBObject coordinates, BasicDBObject location){
		BasicDBList coordPoint = (BasicDBList) coordinates.get("coordinates");
		double coordLng = getDoubleFromCoordinatesItem(coordPoint.get(0));
		double coordLat = getDoubleFromCoordinatesItem(coordPoint.get(1));
		
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
	
	private double getDoubleFromCoordinatesItem(Object coordItem){
		if(coordItem instanceof Double){
			return ((Double)coordItem).doubleValue();
		}
		else if(coordItem instanceof Integer){
			return ((Integer) coordItem).doubleValue();
		}
		else{
			return INVALID_DOUBLE;
		}
	}
	
	private BasicDBList getMiddleOfPolygon(BasicDBList polygon){
		BasicDBList rList = new BasicDBList();
		double lngSum = 0;
		double latSum = 0;
		for(int i = 0; i < polygon.size(); i++){
			BasicDBList point = (BasicDBList) polygon.get(i);
			lngSum += getDoubleFromCoordinatesItem(point.get(0));
			latSum += getDoubleFromCoordinatesItem(point.get(1));
		}
		
		rList.add(lngSum / polygon.size());
		rList.add(latSum / polygon.size());
		return rList;
	}
}

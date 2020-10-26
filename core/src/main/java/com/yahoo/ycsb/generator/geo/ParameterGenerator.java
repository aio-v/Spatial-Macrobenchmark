package com.yahoo.ycsb.generator.geo;

import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.workloads.geo.DataFilter;
import com.yahoo.ycsb.workloads.geo.GeoWorkload;

import java.util.*;
import org.json.*;


/**
 * Author: Yuvraj Kanwar
 *
 * The storage-based generator is fetching pre-generated values/documents from an internal in-memory database instead
 * of generating new random values on the fly.
 * This approach allows YCSB to operate with real (or real-looking) JSON documents rather then synthetic.
 *
 * It also provides the ability to query rich JSON documents by splitting JSON documents into query predicates
 * (field, value, type, field-value relation, logical operation)
 */
public abstract class ParameterGenerator {

  private int totalDocsCountIncidents = 0;
  private int totalDocsCountSchools = 0;
  private int totalDocsCountBuildings = 0;
    
  private int storedDocsCountIncidents = 0;
  
  private int nextInsertDocIdIncidents = 0;
  private int nextInsertDocIdSchools = 0;
  private int nextInsertDocIdBuildings = 0;
  
  private Random rand = new Random();

  private boolean allValuesInitialized = false;
  private Properties properties;
  private int queryLimitMin = 0;
  private int queryLimitMax = 0;
  private int queryOffsetMin = 0;
  private int queryOffsetMax = 0;

  private boolean isZipfian = false;
  private boolean isLatest = false;
  private ZipfianGenerator zipfianGenerator = null;
  
  /* synthesis will result in a grid of size n^2 */
  private int synthesisOffsetCols = 1;  // current column counter var for synthesizing - zero-index
  private int synthesisOffsetRows = 0;  // current row counter var for synthesizing - zero-index
  private static int synthesisOffsetMax; // maximum index of a row or column (n) 

  private DataFilter geoPredicate;
  
  private ArrayList<DataFilter> geometryPredicatesList;

  public static final String GEO_DOCUMENT_PREFIX_INCIDENTS = "incidents";
  public static final String GEO_DOCUMENT_PREFIX_SCHOOLS = "schools";
  public static final String GEO_DOCUMENT_PREFIX_BUILDINGS = "buildings";

  public static final String GEO_SYSTEMFIELD_DELIMITER = ":::";
  public static final String GEO_SYSTEMFIELD_INSERTDOC_COUNTER_INCIDENTS = "GEO_insert_document_counter_incidents";
  public static final String GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_INCIDENTS = "GEO_storage_docs_count_incidents";
  public static final String GEO_SYSTEMFIELD_TOTALDOCS_COUNT_INCIDENTS = "GEO_total_docs_count_incidents";
  
  public static final String GEO_SYSTEMFIELD_INSERTDOC_COUNTER_SCHOOLS = "GEO_insert_document_counter_schools";
  public static final String GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_SCHOOLS = "GEO_storage_docs_count_schools";
  public static final String GEO_SYSTEMFIELD_TOTALDOCS_COUNT_SCHOOLS = "GEO_total_docs_count_schools";
  
  public static final String GEO_SYSTEMFIELD_INSERTDOC_COUNTER_BUILDINGS = "GEO_insert_document_counter_buildings";
  public static final String GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_BUILDINGS = "GEO_storage_docs_count_buildings";
  public static final String GEO_SYSTEMFIELD_TOTALDOCS_COUNT_BUILDINGS = "GEO_total_docs_count_buildings";

  private static final String GEO_METAFIELD_DOCID = "GEO_doc_id";
  private static final String GEO_METAFIELD_INSERTDOC = "GEO_insert_document";

  private static final String GEO_FIELD_INCIDENTS_ID = "_id";
  private static final String GEO_FIELD_INCIDENTS_TYPE = "type";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES = "properties";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID = "OBJECTID";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_NUMBER = "INCIDENT_NUMBER";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_LOCATION = "LOCATION";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_NOTIFICATION = "NOTIFICATION";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_DATE = "INCIDENT_DATE";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_TAG_COUNT = "TAG_COUNT";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_MONIKER_CLASS = "MONIKER_CLASS";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_SQ_FT = "SQ_FT";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_PROP_TYPE = "PROP_TYPE";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_WAIVER = "Waiver";
  private static final String GEO_FIELD_INCIDENTS_GEOMETRY = "geometry";
  private static final String GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_TYPE = "type";
  private static final String GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES = "coordinates";
  
  private static final String GEO_FIELD_SCHOOLS_ID = "_id";
  private static final String GEO_FIELD_SCHOOLS_TYPE = "type";
  private static final String GEO_FIELD_SCHOOLS_PROPERTIES = "properties";
  private static final String GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_OBJECTID = "OBJECTID";
  private static final String GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_NAME = "Name";
  private static final String GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_DESCRIPTION = "description";
  private static final String GEO_FIELD_SCHOOLS_GEOMETRY = "geometry";
  private static final String GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_TYPE = "type";
  private static final String GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_COORDINATES = "coordinates";
  
  private static final String GEO_FIELD_BUILDINGS_ID = "_id";
  private static final String GEO_FIELD_BUILDINGS_TYPE = "type";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES = "properties";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_OBJECTID = "OBJECTID";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_TOPELEV_M = "TOPELEV_M";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_BASEELEV_M = "BASEELEV_M";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_MED_SLOPE = "MED_SLOPE";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_ROOFTYPE = "ROOFTYPE";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_AVGHT_M = "AVGHT_M";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_BASE_M = "BASE_M";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_ORIENT8 = "ORIENT8";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_LEN = "LEN";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_WID = "WID";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_GLOBALID = "GlobalID";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_AREA = "Shape__Area";
  private static final String GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_LENGTH = "Shape__Length";
  private static final String GEO_FIELD_BUILDINGS_GEOMETRY = "geometry";
  private static final String GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_TYPE = "type";
  private static final String GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_COORDINATES = "coordinates";

  private final Map<String, Set<String>> allGeoFields = new HashMap<String, Set<String>>() {{
      put(GEO_DOCUMENT_PREFIX_INCIDENTS, new HashSet<String>() {{
          add(GEO_FIELD_INCIDENTS_ID);
//          add(GEO_FIELD_INCIDENTS_TYPE);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_NUMBER);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_LOCATION);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_NOTIFICATION);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_DATE);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_TAG_COUNT);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_MONIKER_CLASS);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_SQ_FT);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_PROP_TYPE);
//          add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_WAIVER);
//          add(GEO_FIELD_INCIDENTS_GEOMETRY);
//          add(GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_TYPE);
//          add(GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES);
        }});
      put(GEO_DOCUMENT_PREFIX_SCHOOLS, new HashSet<String>() {{
          add(GEO_FIELD_SCHOOLS_ID);
          add(GEO_FIELD_SCHOOLS_TYPE);
          add(GEO_FIELD_SCHOOLS_PROPERTIES);
          add(GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_OBJECTID);
          add(GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_NAME);
          add(GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_DESCRIPTION);
          add(GEO_FIELD_SCHOOLS_GEOMETRY);
          add(GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_TYPE);
          add(GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_COORDINATES);
        }});
      put(GEO_DOCUMENT_PREFIX_BUILDINGS, new HashSet<String>() {{
          add(GEO_FIELD_BUILDINGS_ID);
//          add(GEO_FIELD_BUILDINGS_TYPE);
          add(GEO_FIELD_BUILDINGS_PROPERTIES);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_OBJECTID);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_TOPELEV_M);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_BASEELEV_M);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_MED_SLOPE);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_ROOFTYPE);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_AVGHT_M);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_BASE_M);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_ORIENT8);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_LEN);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_WID);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_GLOBALID);
          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_AREA);
//          add(GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_LENGTH);
//          add(GEO_FIELD_BUILDINGS_GEOMETRY);
//          add(GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_TYPE);
//          add(GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_COORDINATES);
        }});
    }};

  protected abstract Map<String, Object> getBulkVal(Collection<String> keys);
    
  protected abstract void setVal(String key, String value);

  protected abstract String getVal(String key);

  protected abstract int increment(String key, int step);

  public ParameterGenerator(Properties p) {
    properties = p;

    queryLimitMin = Integer.parseInt(p.getProperty(GeoWorkload.GEO_QUERY_LIMIT_MIN,
        GeoWorkload.GEO_QUERY_LIMIT_MIN_DEFAULT));
    queryLimitMax = Integer.parseInt(p.getProperty(GeoWorkload.GEO_QUERY_LIMIT_MAX,
        GeoWorkload.GEO_QUERY_LIMIT_MAX_DEFAULT));
    if (queryLimitMax < queryLimitMin) {
      int buff = queryLimitMax;
      queryLimitMax = queryLimitMin;
      queryLimitMin = buff;
    }

    queryOffsetMin = Integer.parseInt(p.getProperty(GeoWorkload.GEO_QUERY_OFFSET_MIN,
        GeoWorkload.GEO_QUERY_OFFSET_MIN_DEFAULT));
    queryOffsetMax = Integer.parseInt(p.getProperty(GeoWorkload.GEO_QUERY_OFFSET_MAX,
        GeoWorkload.GEO_QUERY_OFFSET_MAX_DEFAULT));
    if (queryOffsetMax < queryOffsetMin) {
      int buff = queryOffsetMax;
      queryOffsetMax = queryOffsetMin;
      queryOffsetMin = buff;
    }

    isZipfian = p.getProperty(GeoWorkload.GEO_REQUEST_DISTRIBUTION,
        GeoWorkload.GEO_REQUEST_DISTRIBUTION_DEFAULT).equals("zipfian");
    isLatest = p.getProperty(GeoWorkload.GEO_REQUEST_DISTRIBUTION,
        GeoWorkload.GEO_REQUEST_DISTRIBUTION_DEFAULT).equals("latest");
    
//    synthesisOffsetMax = (int) Math.round(Math.sqrt(GeoWorkload.getRecordCount()));
    synthesisOffsetMax = Integer.parseInt(p.getProperty(GeoWorkload.DATA_SIZE, 
        ((int)Math.round(Math.sqrt(GeoWorkload.getRecordCount())) + "")));
  }

  public final Map<String, Set<String>> getAllGeoFields() {
    return allGeoFields;
  }

  public void putIncidentsDocument(String docKey, String docBody) throws Exception {
    HashMap<String, String> tokens = tokenize(docBody);
    String prefix = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_INCIDENTS, 1) - 1;

    setVal(prefix + GEO_METAFIELD_DOCID + GEO_SYSTEMFIELD_DELIMITER + storageCount, docKey);
    setVal(prefix + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + storageCount, docBody);

    for (String key : tokens.keySet()){
      String storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + storageCount;
      String value = tokens.get(key);
      if (value != null) {
        setVal(storageKey, value);
      }  else {
        for (int i = (storageCount-1); i>0; i--) {
          String prevKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + i;
          String prevVal = getVal(prevKey);
          if (prevVal != null) {
            setVal(storageKey, prevVal);
            break;
          }
        }
      }
    }

    //make sure all values are initialized
    if ((!allValuesInitialized) && (storageCount > 1)) {
      boolean nullDetected = false;
      for (String key : tokens.keySet()) {
        for (int i = 0; i< storageCount; i++) {
          String storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + i;
          String storageValue = getVal(storageKey);
          if (storageValue != null) {
            for (int j = i; j>=0; j--) {
              storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + j;
              setVal(storageKey, storageValue);
            }
            break;
          } else {
            nullDetected = true;
          }
        }
      }
      allValuesInitialized = !nullDetected;
    }
  }

  public DataFilter getGeoPredicate() {
    return geoPredicate;
  }

  public void buildGeoReadPredicate() {
    String storageKey = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER +
        GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + getIncidentIdWithDistribution();

    String docBody = getVal(storageKey);
    String keyPrefix = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER;
    int docCounter = increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER_INCIDENTS, 1);

    geoPredicate = new DataFilter();
    geoPredicate.setDocid(keyPrefix + docCounter);
    geoPredicate.setValue(docBody);
    DataFilter queryPredicate = new DataFilter();
    queryPredicate.setName(GEO_FIELD_INCIDENTS_GEOMETRY);
    JSONObject obj = new JSONObject(geoPredicate.getValue());
    JSONObject jobj = (JSONObject) obj.get("geometry");
    queryPredicate.setValueA(jobj);

    buildGeoInsertDocument();
    DataFilter queryPredicate2 = new DataFilter();
    queryPredicate2.setName(GEO_FIELD_INCIDENTS_GEOMETRY);
    double[] latLong2 = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    JSONArray jsonArray2 = new JSONArray(latLong2);
    JSONObject jobj2 = new JSONObject().put("type", "Point");
    jobj2.put("coordinates", jsonArray2);
    queryPredicate2.setValueA(jobj2);

    buildGeoInsertDocument();
    DataFilter queryPredicate3 = new DataFilter();
    queryPredicate3.setName(GEO_FIELD_INCIDENTS_GEOMETRY);
    double[] latLong = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    JSONArray jsonArray = new JSONArray(latLong);
    double[] latLong3 = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    double[] latLong4 = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    JSONArray jsonArray3 = new JSONArray(latLong3);
    JSONArray jsonArray4 = new JSONArray(latLong4);
    JSONArray jsonArray5 = new JSONArray();
    JSONArray jsonArray6 = new JSONArray();
    JSONArray jsonArray7 = new JSONArray();
    jsonArray5.put(jsonArray);
    jsonArray5.put(jsonArray2);
    jsonArray6.put(jsonArray3);
    jsonArray6.put(jsonArray4);
    jsonArray7.put(jsonArray5);
    jsonArray7.put(jsonArray6);
    JSONObject jobj3 = new JSONObject().put("type", "MultiLineString");
    jobj3.put("coordinates", jsonArray7);
    queryPredicate3.setValueA(jobj3);
    geoPredicate.setNestedPredicateC(queryPredicate3);
    geoPredicate.setNestedPredicateB(queryPredicate2);
    geoPredicate.setNestedPredicateA(queryPredicate);


  }

  public void buildGeoInsertDocument() {
    String storageKey = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER +
        GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + getNumberRandom(getStoredIncidentsCount());

    String docBody = getVal(storageKey);
    String keyPrefix = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER;
    int docCounter = increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER_INCIDENTS, 1);

    geoPredicate = new DataFilter();
    geoPredicate.setDocid(keyPrefix + docCounter);
    geoPredicate.setValue(docBody);

  }

  public void buildGeoUpdatePredicate() {
    buildGeoInsertDocument();
    DataFilter queryPredicate = new DataFilter();
    queryPredicate.setName(GEO_FIELD_INCIDENTS_GEOMETRY);
    double[] latLong = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    JSONArray jsonArray = new JSONArray(latLong);
    JSONObject jobj = new JSONObject().put("type", "Point");
    jobj.put("coordinates", jsonArray);
    queryPredicate.setValueA(jobj);
    geoPredicate.setNestedPredicateA(queryPredicate);
  }

  public String getIncidentsIdRandom() {
    return "" + getNumberRandom(getTotalIncidentsCount());
  }

  public String getIncidentIdWithDistribution() {
    if (isZipfian) {
      return getNumberZipfianUnifrom(getTotalIncidentsCount())+"";
    }
    if (isLatest) {
      return getNumberZipfianLatests(getTotalIncidentsCount())+"";
    }
    return getIncidentsIdRandom();
  }

  public int getRandomLimit(){
    if (queryLimitMax == queryLimitMin) {
      return queryLimitMax;
    }
    return rand.nextInt(queryLimitMax - queryLimitMin + 1) + queryLimitMin;
  }

  public int getRandomOffset(){

    if (queryOffsetMax == queryOffsetMin) {
      return queryOffsetMax;
    }
    return rand.nextInt(queryOffsetMax - queryOffsetMin + 1) + queryOffsetMin;
  }

  private HashMap<String, String> tokenize(String jsonString) {
    HashMap<String, String> tokens = new HashMap<String, String>();
    JSONObject obj = new JSONObject(jsonString);

    try {
      tokenizeFields(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - plain fields");
      ex.printStackTrace();
    }

    try {
      tokenizeObjects(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - objects");
      ex.printStackTrace();
    }

    return tokens;
  }

  private void tokenizeFields(JSONObject obj, HashMap<String, String> tokens) {


    //string
    ArrayList<String> stringFields = new ArrayList<>(Arrays.asList(GEO_FIELD_INCIDENTS_TYPE));

    for (String field : stringFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, obj.getString(field));
      }
    }
  }


  private void tokenizeObjects(JSONObject obj, HashMap<String, String>  tokens) {

    String id = GEO_FIELD_INCIDENTS_ID;

    tokens.put(GEO_FIELD_INCIDENTS_ID, null);

    if(obj.has(id) && !obj.isNull(id)){
      JSONObject idobj = obj.getJSONObject(id);
      String key = id;
      tokens.put(key, JSONObject.valueToString(idobj));
    }
    //1-level nested objects
    String field = GEO_FIELD_INCIDENTS_PROPERTIES;

    String l1Prefix = field + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_NUMBER, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_LOCATION, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_NOTIFICATION, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_DATE, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_TAG_COUNT, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_MONIKER_CLASS, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_SQ_FT, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_PROP_TYPE, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_WAIVER, null);


    if (obj.has(field) && !obj.isNull(field)) {
      JSONObject inobj = obj.getJSONObject(field);

      ArrayList<String> inobjStringFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_NUMBER,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_LOCATION,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_NOTIFICATION,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_DATE,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_MONIKER_CLASS,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_PROP_TYPE,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_WAIVER));

      for (String infield : inobjStringFields) {
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + GEO_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
        //integer
        ArrayList<String> intFields = new ArrayList<>(Arrays.asList(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID,
            GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_TAG_COUNT, GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_SQ_FT));

        for (String intfield : intFields) {
          if (inobj.has(intfield) && !inobj.isNull(intfield)) {
            String key = field + GEO_SYSTEMFIELD_DELIMITER + intfield;
            tokens.put(key, String.valueOf(inobj.getInt(intfield)));
          }
        }
      }
    }

    //geospatial objects
    String geoField = GEO_FIELD_INCIDENTS_GEOMETRY;

    String lPrefix = geoField + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(lPrefix + GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_TYPE, null);
    tokens.put(lPrefix + GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES, null);


    if (obj.has(geoField) && !obj.isNull(geoField)) {
      JSONObject ingobj = obj.getJSONObject(geoField);

      ArrayList<String> ingeoobjStringFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_TYPE));

      for (String gfield : ingeoobjStringFields) {
        if (ingobj.has(gfield) && !ingobj.isNull(gfield)) {
          String key = geoField + GEO_SYSTEMFIELD_DELIMITER + gfield;
          tokens.put(key, ingobj.getString(gfield));
        }
      }

      String coord = GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES;
      JSONArray arr = ingobj.getJSONArray(coord);
      if (arr.length() > 0) {
        String key = geoField + GEO_SYSTEMFIELD_DELIMITER + coord;
        tokens.put(key, arr.getLong(0)+","+arr.getLong(1));
      }
    }
  }

  private int getStoredIncidentsCount() {
    if (storedDocsCountIncidents == 0) {
      storedDocsCountIncidents = Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER +
          GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_INCIDENTS));
    }
    return storedDocsCountIncidents;
  }

  private int getTotalIncidentsCount() {
    if (totalDocsCountIncidents == 0) {
      totalDocsCountIncidents = Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER +
          GEO_SYSTEMFIELD_TOTALDOCS_COUNT_INCIDENTS));
    }
    return totalDocsCountIncidents;
  }



  private int getNumberZipfianUnifrom(int totalItems) {
    if (zipfianGenerator == null) {
      zipfianGenerator = new ZipfianGenerator(1L, Long.valueOf(getStoredIncidentsCount()-1).longValue());
    }
    return  totalItems - zipfianGenerator.nextValue().intValue();
  }


  //getting latest docId shifted back on (max limit + max offest) to ensure the query returns expected amount of results
  private int getNumberZipfianLatests(int totalItems) {
    if (zipfianGenerator == null) {
      zipfianGenerator = new ZipfianGenerator(1L, Long.valueOf(getStoredIncidentsCount()-1).longValue());
    }
    return  totalItems - zipfianGenerator.nextValue().intValue() - queryLimitMax - queryOffsetMax;
  }

  public int getNumberRandom(int limit) {
    return rand.nextInt(limit);
  }
  
  /*
   * =====================VARIABLE TABLE OPERATIONS===================== 
   */
  
  public int getSynthesisOffsetCols() {
    return synthesisOffsetCols;
  }
  
  public int getSynthesisOffsetRows() {
    return synthesisOffsetRows;
  }

  public static int getSynthesisOffsetMax() {
    return synthesisOffsetMax;
  }

  /**
   * Increment synthesis offset by 1.
   * @return
   */
  public void incrementSynthesisOffset() {
    if(synthesisOffsetCols + 1 == synthesisOffsetMax) {
      synthesisOffsetCols = (synthesisOffsetCols + 1) % synthesisOffsetMax;
      synthesisOffsetRows++;
    } else {
      synthesisOffsetCols++;
    }
  }
  
  /**
   * Get count of total documents in the table.
   * @param table
   * @return
   */
  public int getTotalDocsCount(String table) {
    switch(table) {
    case GEO_DOCUMENT_PREFIX_SCHOOLS:
      if(totalDocsCountSchools == 0) {
        totalDocsCountSchools = Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_SCHOOLS + GEO_SYSTEMFIELD_DELIMITER 
            + GEO_SYSTEMFIELD_TOTALDOCS_COUNT_SCHOOLS));
      }
      return totalDocsCountSchools;
    case GEO_DOCUMENT_PREFIX_BUILDINGS:
      if(totalDocsCountBuildings == 0) {
        totalDocsCountBuildings = Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_BUILDINGS + GEO_SYSTEMFIELD_DELIMITER 
            + GEO_SYSTEMFIELD_TOTALDOCS_COUNT_BUILDINGS));
      }
      return totalDocsCountBuildings;
    default:
      return getTotalIncidentsCount();
    }
  }
  
  /**
   * Get stored count of documents in the table.
   * @return
   */
  public int getStoredDocsCount(String table) {
    switch(table) {
    case GEO_DOCUMENT_PREFIX_SCHOOLS:
      return Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_SCHOOLS + GEO_SYSTEMFIELD_DELIMITER 
            + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_SCHOOLS));
    case GEO_DOCUMENT_PREFIX_BUILDINGS:
      return Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_BUILDINGS + GEO_SYSTEMFIELD_DELIMITER 
            + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_BUILDINGS));
    default:
      return Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER
            + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_INCIDENTS));
    }
  }
  
  /**
   * Get a random ID from the table.
   * @param table
   * @return
   */
  public String getIdRandom(String table) {
    return "" + getNumberRandom(getTotalDocsCount(table));
  }
  
  /**
   * Get the next ID from the table. Loops back to 0 when it hits the last ID.
   * @param table
   * @return the next ID
   */
  public String getNextId(String table) {
    String nextId = "";
    switch(table) {
    case GEO_DOCUMENT_PREFIX_SCHOOLS:
      // return the next ID shifted up by 1, because object IDs start at 1
      nextId += (nextInsertDocIdSchools + 1);
      // increment
      nextInsertDocIdSchools = (nextInsertDocIdSchools + 1) % getTotalDocsCount(GEO_DOCUMENT_PREFIX_SCHOOLS); 
      break;
    case GEO_DOCUMENT_PREFIX_BUILDINGS:
      nextId += (nextInsertDocIdBuildings + 1);
      nextInsertDocIdBuildings = (nextInsertDocIdBuildings + 1) % getTotalDocsCount(GEO_DOCUMENT_PREFIX_BUILDINGS);
      break;
    default:
      nextId += (nextInsertDocIdIncidents + 1);
      nextInsertDocIdIncidents = (nextInsertDocIdIncidents + 1) % getTotalDocsCount(GEO_DOCUMENT_PREFIX_INCIDENTS);
      break;
    }
    return nextId;
  }
  
  /**
   * Tokenizes a jsonString based on table.
   * @param table
   * @param jsonString
   * @return
   */
  private HashMap<String, String> tokenize(String table, String jsonString) {
    HashMap<String, String> tokens = new HashMap<String, String>();
    JSONObject obj = new JSONObject(jsonString);
    try {
      tokenizeFields(table, obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - plain fields");
      ex.printStackTrace();
    }

    try {
      tokenizeObjects(table, obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - objects");
      ex.printStackTrace();
    }

    return tokens;
  }
  
  /**
   * Puts a document into the cache.
   * @param table
   * @param docKey
   * @param docBody
   * @throws Exception
   * @return inserted doc id
   */
  public int putDocument(String table, String docKey, String docBody) throws Exception {
    HashMap<String, String> tokens = tokenize(table, docBody);
    
    String prefix = "";   // set prefix & storagecount depending on table
    int storageCount = 0;
    switch(table) {
    case GEO_DOCUMENT_PREFIX_SCHOOLS:
      prefix = GEO_DOCUMENT_PREFIX_SCHOOLS + GEO_SYSTEMFIELD_DELIMITER;
      storageCount = increment(prefix + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_SCHOOLS, 1);
      break;
    case GEO_DOCUMENT_PREFIX_BUILDINGS:
      prefix = GEO_DOCUMENT_PREFIX_BUILDINGS + GEO_SYSTEMFIELD_DELIMITER;
      storageCount = increment(prefix + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_BUILDINGS, 1);
      break;
    default:
      prefix = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER;
      storageCount = increment(prefix + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_INCIDENTS, 1);
      break;
    }
    
    // add to memcached client
    setVal(prefix + GEO_METAFIELD_DOCID + GEO_SYSTEMFIELD_DELIMITER + docKey, docKey);      
    setVal(prefix + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + docKey, docBody);

//    // adds fields to memcached client
//    for (String key : tokens.keySet()){
//      String storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + storageCount;
//      String value = tokens.get(key);
//      if (value != null) {
//        setVal(storageKey, value);
//      }
//    }
    
    // returns the doc id that was just inserted
    return storageCount;
  }
  
  /**
   * Gets a document from memcached and prepares it to become the predicate of a GeoInsert.
   * @param table
   * @return the new doc body
   */
  public String buildGeoInsertDocument(String table, int key, String generatedId) {
    String storageKey = "";
    String keyPrefix = "";
    String docBody = null;
    int docCounter = 0;
    switch(table) {
    case GEO_DOCUMENT_PREFIX_SCHOOLS:
      keyPrefix = GEO_DOCUMENT_PREFIX_SCHOOLS + GEO_SYSTEMFIELD_DELIMITER;
      storageKey = keyPrefix + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + 
          key;
      while(docBody == null) {
        docBody = getVal(storageKey);
      }
      docCounter = increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER_SCHOOLS, 1);
      break;
    case GEO_DOCUMENT_PREFIX_BUILDINGS:
      keyPrefix = GEO_DOCUMENT_PREFIX_BUILDINGS + GEO_SYSTEMFIELD_DELIMITER;
      storageKey = keyPrefix + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + 
          key;
      while(docBody == null) {
        System.out.println("STALL");
        docBody = getVal(storageKey);
      }
      docCounter = increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER_BUILDINGS, 1);
      break;
    default:
      keyPrefix = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER;
      storageKey = keyPrefix + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + 
          key;
      while(docBody == null) {
        docBody = getVal(storageKey);
      }
      docCounter = increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER_INCIDENTS, 1);
    }
    
    // Synthesize data
    String newDocBody = synthesize(table, docBody, generatedId);
    
//    geoPredicate = new DataFilter();
//    geoPredicate.setDocid(keyPrefix + docCounter);
//    geoPredicate.setValue(newDocBody);
    
    return newDocBody;
  }
  
  /**
   * Tokenizes fields based on a table.
   * @param table
   * @param obj
   * @param tokens
   */
  private void tokenizeFields(String table, JSONObject obj, HashMap<String, String> tokens) {
    // array list of fields
    ArrayList<String> stringFields;
    switch(table) {
    case GEO_DOCUMENT_PREFIX_SCHOOLS:     // SCHOOLS table fields
      stringFields = new ArrayList<>(Arrays.asList(GEO_FIELD_SCHOOLS_TYPE));
      break;
    case GEO_DOCUMENT_PREFIX_BUILDINGS:   // BUILDINGS table fields
      stringFields = new ArrayList<>(Arrays.asList(GEO_FIELD_BUILDINGS_TYPE));
      break;
    default:                              // INCIDENTS table fields
      stringFields = new ArrayList<>(Arrays.asList(GEO_FIELD_INCIDENTS_TYPE));
      break;
    }     

    // for every field, add a token
    for (String field : stringFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, obj.getString(field));
      }
    }
  }

  private void tokenizeObjects(String table, JSONObject obj, HashMap<String, String> tokens) {
    switch(table) {
    case GEO_DOCUMENT_PREFIX_SCHOOLS:
      tokenizeSchoolsObjects(obj, tokens);
      break;
    case GEO_DOCUMENT_PREFIX_BUILDINGS:
      tokenizeBuildingsObjects(obj, tokens);
      break;
    default:
      tokenizeObjects(obj, tokens);
      break;
    }
  }
  
  /**
   * Helper method to tokenize the schools table.
   * @param obj
   * @param tokens
   */
  private void tokenizeSchoolsObjects(JSONObject obj, HashMap<String, String> tokens) {
    // ID field
    String id = GEO_FIELD_SCHOOLS_ID;
    tokens.put(GEO_FIELD_SCHOOLS_ID, null);               // add as token
    if(obj.has(id) && !obj.isNull(id)) {                  // set value
      JSONObject idobj = obj.getJSONObject(id);
      String key = id;
      tokens.put(key, JSONObject.valueToString(idobj));
    }

    // 1-level nested objects
    String field = GEO_FIELD_SCHOOLS_PROPERTIES;
    String l1Prefix = field + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(l1Prefix + GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_OBJECTID, null);   // add as tokens
    tokens.put(l1Prefix + GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_NAME, null);
    tokens.put(l1Prefix + GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_DESCRIPTION, null);

    if (obj.has(field) && !obj.isNull(field)) {               // get properties
      JSONObject inobj = obj.getJSONObject(field);

      // get string properties objects
      ArrayList<String> inobjStringFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_NAME,
          GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_DESCRIPTION));

      // strings
      for (String infield : inobjStringFields) {                // set values to tokens
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + GEO_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
      }
      
      // get integer properties objects
      ArrayList<String> intFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_OBJECTID));

      // integers
      for (String intfield : intFields) {                 // set values to tokens
        if (inobj.has(intfield) && !inobj.isNull(intfield)) {
          String key = field + GEO_SYSTEMFIELD_DELIMITER + intfield;
          tokens.put(key, String.valueOf(inobj.getInt(intfield)));
        }
      }
    }

    //geospatial objects
    String geoField = GEO_FIELD_SCHOOLS_GEOMETRY;
    String lPrefix = geoField + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(lPrefix + GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_TYPE, null);      // add as tokens
    tokens.put(lPrefix + GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_COORDINATES, null);

    if (obj.has(geoField) && !obj.isNull(geoField)) {             // get geometry
      JSONObject ingobj = obj.getJSONObject(geoField);
      
      // get string geometry objects
      ArrayList<String> ingeoobjStringFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_TYPE));

      for (String gfield : ingeoobjStringFields) {              // set values to tokens
        if (ingobj.has(gfield) && !ingobj.isNull(gfield)) {
          String key = geoField + GEO_SYSTEMFIELD_DELIMITER + gfield;
          tokens.put(key, ingobj.getString(gfield));
        }
      }

      String coord = GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_COORDINATES;        // get geometry coordinates
      JSONArray arr = ingobj.getJSONArray(coord);
      if (arr.length() > 0) {
        String key = geoField + GEO_SYSTEMFIELD_DELIMITER + coord;
        tokens.put(key, arr.getLong(0) + "," + arr.getLong(1));       // set value
      }
    }
  }
  
  /**
   * Helper method to tokenize the buildings table.
   * @param obj
   * @param tokens
   */
  private void tokenizeBuildingsObjects(JSONObject obj, HashMap<String, String> tokens) {
    // ID field
    String id = GEO_FIELD_BUILDINGS_ID;
    tokens.put(GEO_FIELD_BUILDINGS_ID, null);         // add as token
    if(obj.has(id) && !obj.isNull(id)) {  // set value
      JSONObject idobj = obj.getJSONObject(id);
      String key = id;
      tokens.put(key, JSONObject.valueToString(idobj));
    }

    // 1-level nested objects
    String field = GEO_FIELD_BUILDINGS_PROPERTIES;
    String l1Prefix = field + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_OBJECTID, null);   // add as tokens
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_TOPELEV_M, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_BASEELEV_M, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_MED_SLOPE, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_ROOFTYPE, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_AVGHT_M, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_BASE_M, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_ORIENT8, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_LEN, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_WID, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_GLOBALID, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_AREA, null);
    tokens.put(l1Prefix + GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_LENGTH, null);

    if (obj.has(field) && !obj.isNull(field)) {               // get properties
      JSONObject inobj = obj.getJSONObject(field);

      // get string properties objects
      ArrayList<String> inobjStringFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_TOPELEV_M,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_BASEELEV_M,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_MED_SLOPE,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_ROOFTYPE,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_AVGHT_M,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_BASE_M,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_ORIENT8,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_LEN,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_WID,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_GLOBALID));

      // strings
      for (String infield : inobjStringFields) {                // set values to tokens
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + GEO_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
      }
      
      // get integer properties objects
      ArrayList<String> intFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_OBJECTID,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_AREA,
          GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_LENGTH));

      // integers
      for (String intfield : intFields) {                 // set values to tokens
        if (inobj.has(intfield) && !inobj.isNull(intfield)) {
          String key = field + GEO_SYSTEMFIELD_DELIMITER + intfield;
          tokens.put(key, String.valueOf(inobj.getInt(intfield)));
        }
      }
    }

    //geospatial objects
    String geoField = GEO_FIELD_BUILDINGS_GEOMETRY;
    String lPrefix = geoField + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(lPrefix + GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_TYPE, null);      // add as tokens
    tokens.put(lPrefix + GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_COORDINATES, null);

    if (obj.has(geoField) && !obj.isNull(geoField)) {             // get geometry
      JSONObject ingobj = obj.getJSONObject(geoField);

      // get string geometry objects
      ArrayList<String> ingeoobjStringFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_TYPE));

      for (String gfield : ingeoobjStringFields) {                // set values to tokens
        if (ingobj.has(gfield) && !ingobj.isNull(gfield)) {
          String key = geoField + GEO_SYSTEMFIELD_DELIMITER + gfield;
          tokens.put(key, ingobj.getString(gfield));
        }
      }

      String coord = GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_COORDINATES;    // get geometry coordinates
      JSONArray arr = ingobj.getJSONArray(coord);
      String key = geoField + GEO_SYSTEMFIELD_DELIMITER + coord;
      String coordstr = "";
      // polygon
      for(int i = 0; i < arr.getJSONArray(0).length(); i++) {
        JSONArray point = arr.getJSONArray(0).getJSONArray(i);
        coordstr += point.getDouble(0) + "," + point.getDouble(1) + " ";
      }
      
      tokens.put(key, coordstr);       // set value
    }
  }  
  
  /**
   * Synthesize data by shifting geometry coordinates and changing object ID.
   * @param jsonString
   * @return
   */
  private String synthesize(String table, String jsonString, String generatedId) {
    JSONObject obj = new JSONObject(jsonString);
    String geometryField = "";
    String coordinatesField = "";
    String propertiesField = "";
    String objectIdField = "";
    String idField = "";
    boolean polygon = false;
    
    switch(table) {
    case GEO_DOCUMENT_PREFIX_SCHOOLS:
      geometryField = GEO_FIELD_SCHOOLS_GEOMETRY;
      coordinatesField = GEO_FIELD_SCHOOLS_GEOMETRY_OBJ_COORDINATES;
      propertiesField = GEO_FIELD_SCHOOLS_PROPERTIES;
      objectIdField = GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_OBJECTID;
      idField = GEO_FIELD_SCHOOLS_ID;
      break;
    case GEO_DOCUMENT_PREFIX_BUILDINGS:
      geometryField = GEO_FIELD_BUILDINGS_GEOMETRY;
      coordinatesField = GEO_FIELD_BUILDINGS_GEOMETRY_OBJ_COORDINATES;
      propertiesField = GEO_FIELD_BUILDINGS_PROPERTIES;
      objectIdField = GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_OBJECTID;
      idField = GEO_FIELD_BUILDINGS_ID;
      polygon = true;
      break;
    default:
      geometryField = GEO_FIELD_INCIDENTS_GEOMETRY;
      coordinatesField = GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES;
      propertiesField = GEO_FIELD_INCIDENTS_PROPERTIES;
      objectIdField = GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID;
      idField = GEO_FIELD_INCIDENTS_ID;
      break;
    }
    
    if(obj.has(geometryField) && !obj.isNull(geometryField)) {
      // get coordinates array
      JSONArray arr = obj.getJSONObject(geometryField).getJSONArray(coordinatesField);
      if(arr.length() > 0) {
        // shift coordinates
        JSONArray newArr = null;
        if(!polygon) {      // check for nested array
          double[] newCoords = {arr.getDouble(0) + (GeoWorkload.LONG_OFFSET * synthesisOffsetCols), 
              arr.getDouble(1) + (GeoWorkload.LAT_OFFSET * synthesisOffsetRows)};
          newArr = new JSONArray(newCoords);
        } else {
          JSONArray temp = new JSONArray();
          for(int i = 0; i < arr.length(); i++) {
            JSONArray filler = arr.getJSONArray(i);
            for(int j = 0; j < filler.length(); j++) {
              double[] newCoords = {
                  filler.getJSONArray(j).getDouble(0) + (GeoWorkload.LONG_OFFSET * synthesisOffsetCols),
                  filler.getJSONArray(j).getDouble(1) + (GeoWorkload.LAT_OFFSET * synthesisOffsetRows)
              };
              temp.put(new JSONArray(newCoords));
            }
          }
          newArr = new JSONArray();
          newArr.put(temp);
        }
        obj.getJSONObject(geometryField).put(coordinatesField, newArr);
      }
    }
    
    if(obj.has(propertiesField) && !obj.isNull(propertiesField)) {
      // get objectID
      int objId = obj.getJSONObject(propertiesField).getInt(objectIdField);
      // modify
      int synthOffset = (synthesisOffsetRows * synthesisOffsetMax) + synthesisOffsetCols;
      objId = objId + (getTotalDocsCount(table) * synthOffset);
      obj.getJSONObject(propertiesField).put(objectIdField, objId);
    }
    
    if(obj.has(idField) && !obj.isNull(idField)) {
      // set _id to new, unique generated id
      obj.getJSONObject(idField).put("$oid", generatedId);
    }
    return obj.toString();
  }
  
  /**
   * Builds the predicates necessary for use case 1.
   */
  public void buildGeoPredicateCase1() {
    if(geometryPredicatesList != null) {
      return;
    }
    geometryPredicatesList = new ArrayList<DataFilter>();
    
    // Create list of all schools to bulk get
    ArrayList<String> keys = new ArrayList<String>();
    Random random = new Random();
    int docCount = getTotalDocsCount(GEO_DOCUMENT_PREFIX_SCHOOLS) * synthesisOffsetMax * synthesisOffsetMax;
//    
////    for(int i = 0; i < 3; i++) {
//    int nextSchool = random.nextInt(2754);
//    String key = GEO_DOCUMENT_PREFIX_SCHOOLS + GEO_SYSTEMFIELD_DELIMITER 
//        + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + nextSchool;
//    String val = getVal(key);
//
//    // parse document - get geometry
//    DataFilter queryPredicate = new DataFilter();
//    queryPredicate.setName(GEO_FIELD_SCHOOLS_GEOMETRY);
//    JSONObject obj = new JSONObject(val);
//    JSONObject jobj = (JSONObject) obj.get(GEO_FIELD_SCHOOLS_GEOMETRY);
//    queryPredicate.setValueA(jobj);
//
//    // original school document
//    DataFilter schoolPredicate = new DataFilter();
////    schoolPredicate.setDocid(key);
////    schoolPredicate.setValue(val);
//    schoolPredicate.setName(((JSONObject)obj.get(GEO_FIELD_SCHOOLS_PROPERTIES)).
//        getString(GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_NAME));
//    // attach specific query predicate
//    schoolPredicate.setNestedPredicateA(queryPredicate);
//    
//    System.out.println(queryPredicate.getValueA());
//
//    // add school to list
//    geometryPredicatesList.add(schoolPredicate);
//    }
    
    for(int i = 1; i <= docCount; i++) {
      keys.add(GEO_DOCUMENT_PREFIX_SCHOOLS + GEO_SYSTEMFIELD_DELIMITER 
          + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + i);
    }
    
    // Get documents from memcached
    Map<String, Object> bulk = null;
    while(bulk == null) {
      System.out.println("NULL: " + keys.size());
      bulk = getBulkVal(keys);
    }
    
    for(String key : bulk.keySet()) {
      // parse document - get geometry
      DataFilter queryPredicate = new DataFilter();
      queryPredicate.setName(GEO_FIELD_SCHOOLS_GEOMETRY);
      JSONObject obj = new JSONObject(bulk.get(key).toString());
      JSONObject jobj = (JSONObject) obj.get(GEO_FIELD_SCHOOLS_GEOMETRY);
      queryPredicate.setValueA(jobj);
      
      // original school document
      DataFilter schoolPredicate = new DataFilter();
      schoolPredicate.setDocid(key);
      schoolPredicate.setValue(bulk.get(key).toString());
      schoolPredicate.setName(((JSONObject)obj.get(GEO_FIELD_SCHOOLS_PROPERTIES)).
          getString(GEO_FIELD_SCHOOLS_PROPERTIES_OBJ_NAME));
      // attach specific query predicate
      schoolPredicate.setNestedPredicateA(queryPredicate);
      
      // add school to list
      geometryPredicatesList.add(schoolPredicate);
    }
    // randomize order
    Collections.shuffle(geometryPredicatesList, random);
  }
  
  /**
   * Builds the predicates necessary for use case 3.
   */
  public void buildGeoPredicateCase3() {
    geometryPredicatesList = new ArrayList<DataFilter>();
    
    // calculate grid cells length and width
    double longUnitShift = GeoWorkload.LONG_OFFSET / (GeoWorkload.GRID_COLS * synthesisOffsetMax);
    double latUnitShift = GeoWorkload.LAT_OFFSET / (GeoWorkload.GRID_ROWS * synthesisOffsetMax);
    
    for(int i = 0; i < GeoWorkload.GRID_ROWS * synthesisOffsetMax; i++) {
      for(int j = 0; j < GeoWorkload.GRID_COLS * synthesisOffsetMax; j++) {
        // calculate bottom left and upper right points of grid cell
        double[] bottomLeft = {GeoWorkload.LONG_MIN + longUnitShift * j, 
            GeoWorkload.LAT_MIN + latUnitShift * i};
        double[] upperLeft = {GeoWorkload.LONG_MIN + longUnitShift * j, 
            GeoWorkload.LAT_MIN + latUnitShift * (i + 1)};
        double[] upperRight = {GeoWorkload.LONG_MIN + longUnitShift * (j + 1),
            GeoWorkload.LAT_MIN + latUnitShift * (i + 1)};
        double[] bottomRight = {GeoWorkload.LONG_MIN + longUnitShift * (j + 1),
            GeoWorkload.LAT_MIN + latUnitShift * i};
        
        // add coordinates
        JSONArray point1 = new JSONArray(bottomLeft);
        JSONArray point2 = new JSONArray(upperLeft);
        JSONArray point3 = new JSONArray(upperRight);
        JSONArray point4 = new JSONArray(bottomRight);
        JSONArray point5 = new JSONArray(bottomLeft);
        JSONArray wrapper1 = new JSONArray();
        JSONArray wrapper2 = new JSONArray();
        wrapper1.put(point1);
        wrapper1.put(point2);
        wrapper1.put(point3);
        wrapper1.put(point4);
        wrapper1.put(point5);
        wrapper2.put(wrapper1);
        JSONObject jobj = new JSONObject().put("type", "Polygon");
        jobj.put("coordinates", wrapper2);
        
        // add to predicates
        DataFilter queryPredicate = new DataFilter();
        queryPredicate.setName("geometry");
        queryPredicate.setValueA(jobj);
        
        geometryPredicatesList.add(queryPredicate);
      }
    }
    // randomize order
    Collections.shuffle(geometryPredicatesList, new Random());
  }
  
  public ArrayList<DataFilter> getGeometryPredicatesList() {
    return geometryPredicatesList;
  }
  
  public String getBuildingsShapeArea() {
    return GEO_FIELD_BUILDINGS_PROPERTIES_OBJ_SHAPE_AREA;
  }
}

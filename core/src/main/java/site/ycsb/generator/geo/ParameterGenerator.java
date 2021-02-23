package site.ycsb.generator.geo;

import site.ycsb.generator.ZipfianGenerator;
import site.ycsb.workloads.geo.DataFilter;
import site.ycsb.workloads.geo.GeoWorkload;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.*;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Author: Tsz Ting Yu & Yvonne Hoang
 *
 * The storage-based generator is fetching pre-generated values/documents from
 * an internal in-memory database instead of generating new random values on the
 * fly. This approach allows YCSB to operate with real (or real-looking) JSON
 * documents rather then synthetic.
 *
 * It also provides the ability to query rich JSON documents by splitting JSON
 * documents into query predicates (field, value, type, field-value relation,
 * logical operation)
 * 
 * For Japan Counties & Bus Route data sets.
 */

public abstract class ParameterGenerator {
  /*------------------METADATA FIELDS--------------------*/
  private int totalDocsCountCounties = 0;
  private int totalDocsCountRoutes = 0;
  
  private AtomicInteger nextLoadDocKeyCounties = new AtomicInteger(0);
  private AtomicInteger nextLoadDocKeyRoutes = new AtomicInteger(0);
  private AtomicInteger nextGeoKeyCounties = new AtomicInteger(0);  
  
  private int nextInsertDocKeyCounties = 0;
  private int nextInsertDocKeyRoutes = 0;
  
  private static final double CHANCE_TO_ADD_DOC_AS_PARAMETER = .05;
  
  private Properties properties;
  private int queryLimitMin = 0;
  private int queryLimitMax = 0;
  private int queryOffsetMin = 0;
  private int queryOffsetMax = 0;

  private boolean isZipfian = false;
  private boolean isLatest = false;
  private ZipfianGenerator zipfianGenerator = null;
  
  /*------------------SYNTHESIS FIELDS-------------------*/
  /*-----synthesis will result in a grid of size n^2-----*/
  private int synthesisOffsetCols = 1;  // current column counter var for synthesizing - zero-index
  private int synthesisOffsetRows = 0;  // current row counter var for synthesizing - zero-index
  private static int synthesisOffsetMax; // maximum index of a row or column (n) 
  
  /*------------------DOCUMENT FIELDS--------------------*/
  public static final String GEO_DOCUMENT_PREFIX_COUNTIES = "counties";
  public static final String GEO_DOCUMENT_PREFIX_ROUTES = "routes";
  
  public static final String GEO_SYSTEMFIELD_DOCUMENT = "doc";
  public static final String GEO_SYSTEMFIELD_GEOMETRY = "geo";
  public static final String GEO_SYSTEMFIELD_DELIMITER = ":::";
  
  public static final String GEO_SYSTEMFIELD_INSERTDOC_COUNTER = "GEO_insert_document_counter";
  public static final String GEO_SYSTEMFIELD_STORAGEDOCS_COUNT = "GEO_storage_docs_count";
  public static final String GEO_SYSTEMFIELD_TOTALDOCS_COUNT = "GEO_total_docs_count";
  public static final String GEO_SYSTEMFIELD_STORAGEGEO_COUNT = "GEO_storage_geo_count";
  
  // Document fields in case an attribute is needed for query/projection
  private static final String GEO_FIELD_COUNTIES_TYPE = "type";
  private static final String GEO_FIELD_COUNTIES_ID = "id";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES = "properties";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_001 = "N03_001";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_002 = "N03_002";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_003 = "N03_003";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_004 = "N03_004";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_007 = "N03_007";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_COLOR = "_color";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_OPACITY = "_opacity";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_WEIGHT = "_weight";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_FILLCOLOR = "_fillColor";
  private static final String GEO_FIELD_COUNTIES_PROPERTIES_OBJ_FILLOPACITY = "_fillOpacity";
  private static final String GEO_FIELD_COUNTIES_GEOMETRY = "geometry";
  private static final String GEO_FIELD_COUNTIES_GEOMETRY_OBJ_TYPE = "type";
  private static final String GEO_FIELD_COUNTIES_GEOMETRY_OBJ_COORDINATES = "coordinates";
  
  private static final String GEO_FIELD_ROUTES_TYPE = "type";
  private static final String GEO_FIELD_ROUTES_PROPERTIES = "properties";
  private static final String GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_001 = "N07_001";
  private static final String GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_002 = "N07_002";
  private static final String GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_003 = "N07_003";
  private static final String GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_004 = "N07_004";
  private static final String GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_005 = "N07_005";
  private static final String GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_006 = "N07_006";
  private static final String GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_007 = "N07_007";
  private static final String GEO_FIELD_ROUTES_GEOMETRY = "geometry";
  private static final String GEO_FIELD_ROUTES_GEOMETRY_OBJ_TYPE = "type";
  private static final String GEO_FIELD_ROUTES_GEOMETRY_OBJ_COORDINATES = "coordinates";
  
  private final Map<String, Set<String>> allGeoFields = new HashMap<String, Set<String>>() {{
    put(GEO_DOCUMENT_PREFIX_COUNTIES, new HashSet<String>() {{
        add(GEO_FIELD_COUNTIES_TYPE);
        add(GEO_FIELD_COUNTIES_ID);
        add(GEO_FIELD_COUNTIES_PROPERTIES);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_001);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_002);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_003);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_004);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_N03_007);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_COLOR);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_OPACITY);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_WEIGHT);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_FILLCOLOR);
        add(GEO_FIELD_COUNTIES_PROPERTIES_OBJ_FILLOPACITY);
        add(GEO_FIELD_COUNTIES_GEOMETRY);
        add(GEO_FIELD_COUNTIES_GEOMETRY_OBJ_TYPE);
        add(GEO_FIELD_COUNTIES_GEOMETRY_OBJ_COORDINATES);
      }});
    put(GEO_DOCUMENT_PREFIX_ROUTES, new HashSet<String>() {{
        add(GEO_FIELD_ROUTES_TYPE);
        add(GEO_FIELD_ROUTES_PROPERTIES);
        add(GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_001);
        add(GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_002);
        add(GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_003);
        add(GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_004);
        add(GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_005);
        add(GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_006);
        add(GEO_FIELD_ROUTES_PROPERTIES_OBJ_N07_007);
        add(GEO_FIELD_ROUTES_GEOMETRY);
        add(GEO_FIELD_ROUTES_GEOMETRY_OBJ_TYPE);
        add(GEO_FIELD_ROUTES_GEOMETRY_OBJ_COORDINATES);
      }});
  }};
  
  /*---------------PARAMETER GENERATION FIELDS-----------------*/
  private DataFilter geoPredicate;
  
  protected abstract Map<String, Object> getBulkVal(Collection<String> keys);
  
  protected abstract void setVal(String key, String value);

  protected abstract String getVal(String key);

  protected abstract int increment(String key, int step);
  
  /*-----------------------------------------------------------*/
  /*----------------------CONSTRUCTOR--------------------------*/
  /*-----------------------------------------------------------*/
  
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
  
  /*-----------------------------------------------------------*/
  /*-------------------------GETTERS---------------------------*/
  /*-----------------------------------------------------------*/
  public final Map<String, Set<String>> getAllGeoFields() {
    return allGeoFields;
  }
  
  /**
   * Get count of total documents in the table.
   * @param table
   * @return
   */
  public int getTotalDocsCount(String table) {
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      if(totalDocsCountCounties == 0) {
        totalDocsCountCounties = Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER 
            + GEO_SYSTEMFIELD_TOTALDOCS_COUNT));
      }
      return totalDocsCountCounties;
    case GEO_DOCUMENT_PREFIX_ROUTES:
      if(totalDocsCountRoutes == 0) {
        totalDocsCountRoutes = Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_ROUTES + GEO_SYSTEMFIELD_DELIMITER 
            + GEO_SYSTEMFIELD_TOTALDOCS_COUNT));
      }
      return totalDocsCountRoutes;
    default:
      return 0;
    }
  }
  
  /**
   * Get stored count of documents in the cache.
   * @return
   */
  public int getStoredDocsCount(String table) {
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      return Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER 
            + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT));
    case GEO_DOCUMENT_PREFIX_ROUTES:
      return Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_ROUTES + GEO_SYSTEMFIELD_DELIMITER
            + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT));
    default:
      return 0;
    }
  }
  
  /**
   * Get stored count of geometries in the cache.
   * @return
   */
  public int getStoredGeoCount(String table) {
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      return Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER 
            + GEO_SYSTEMFIELD_STORAGEGEO_COUNT));
    default:
      return 0;
    }
  }
   
  /*-----------------------------------------------------------*/
  /*----------------------MEMCACHED LOGIC----------------------*/
  /*-----------------------------------------------------------*/
  /**
   * Puts an entire json document into the cache. Intended use for original dataset documents only.
   * @param table
   * @param docKey
   * @param docBody
   * @throws Exception
   * @return inserted doc id
   */
  public int putOriginalDocument(String table, String docBody) {
    String prefix = "";   // set prefix & storagecount depending on table
    int storageCount = 0;
    int nextDocKey = 0;
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      prefix = GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER + GEO_SYSTEMFIELD_DOCUMENT;
      storageCount = increment(GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT, 1);
      nextDocKey = nextLoadDocKeyCounties.getAndIncrement();
      break;
    case GEO_DOCUMENT_PREFIX_ROUTES:
      prefix = GEO_DOCUMENT_PREFIX_ROUTES + GEO_SYSTEMFIELD_DELIMITER + GEO_SYSTEMFIELD_DOCUMENT;
      storageCount = increment(GEO_DOCUMENT_PREFIX_ROUTES + GEO_SYSTEMFIELD_DELIMITER + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT, 1);
      nextDocKey = nextLoadDocKeyRoutes.getAndIncrement();
      break;
    default:
      break;
    }
    
    // add to memcached client
    // e.g. counties:::doc:::1 -> json doc
    setVal(prefix + GEO_SYSTEMFIELD_DELIMITER + nextDocKey, docBody);
    
    // there is a chance to add the geometry to memcached as a potential parameter:
    rollChanceToAddDocAsParameter(table, docBody);
    
    // returns the doc id that was just inserted
    return storageCount;
  }
  
  /**
   * Puts a json geometry field from counties dataset into the cache, 
   * which will serve as parameters for benchmarking queries during run phase.
   * @param table
   * @param docKey
   * @param docBody
   * @throws Exception
   * @return inserted doc id
   */
  public int putGeometryDocument(String table, String docBody) {
    String prefix = "";   // set prefix & storagecount depending on table
    int storageCount = 0;
    int nextGeoKey = 0;
    String docGeometry = null;
    
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      // turn docBody to JSON
      JSONObject jsonDoc = new JSONObject(docBody);
      
      // check if geometry field exists
      if (jsonDoc.has(GEO_FIELD_COUNTIES_GEOMETRY) && !jsonDoc.isNull(GEO_FIELD_COUNTIES_GEOMETRY)) {
        JSONObject geoObj = jsonDoc.getJSONObject(GEO_FIELD_COUNTIES_GEOMETRY);     // extract geometry field
        docGeometry = geoObj.toString();
        
        prefix = GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER + GEO_SYSTEMFIELD_GEOMETRY;
      }
      break;
    default:
      break;
    }
    
    // add to memcached client
    // e.g. counties:::geo:::1 -> geometry json object
    if(docGeometry != null) {
      nextGeoKey = nextGeoKeyCounties.getAndIncrement();
      System.out.println("ADDED " + nextGeoKey);
      storageCount = increment(GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER + GEO_SYSTEMFIELD_STORAGEGEO_COUNT, 1);    // increment counter
      setVal(prefix + GEO_SYSTEMFIELD_DELIMITER + nextGeoKey, docGeometry);
    } else {
      System.out.println("FAILED TO ADD " + nextGeoKey);
    }
    
    // returns the doc id that was just inserted
    return storageCount;
  }
  
  private boolean rollChanceToAddDocAsParameter(String table, String docBody) {
    if(Math.random() < CHANCE_TO_ADD_DOC_AS_PARAMETER) {
      putGeometryDocument(table, docBody);
      return true;
    }
    return false;
  }
  
  /**
   * Fetches JSON document from original dataset from memcached.
   * @param table
   * @param docKey
   * @return
   */
  public String getOriginalDocument(String table, String docKey) {
    String prefix = "";   // set prefix & storagecount depending on table
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      prefix = GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER;
      break;
    case GEO_DOCUMENT_PREFIX_ROUTES:
      prefix = GEO_DOCUMENT_PREFIX_ROUTES + GEO_SYSTEMFIELD_DELIMITER;
      break;
    }
    
    return getVal(prefix + GEO_SYSTEMFIELD_DOCUMENT + GEO_SYSTEMFIELD_DELIMITER + docKey);
  }
  
  /**
   * Fetches a geoJSON geometry field from memcached.
   * @param table
   * @param geoKey
   * @return
   */
  public String getGeometryDocument(String table, String geoKey) {
    String prefix = "";
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      prefix = GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER;
      break;
    default:
      break;
    }
    
    return getVal(prefix + GEO_SYSTEMFIELD_GEOMETRY + GEO_SYSTEMFIELD_DELIMITER + geoKey);
  }
  
  /*-----------------------------------------------------------*/
  /*----------------------SYNTHESIS LOGIC----------------------*/
  /*-----------------------------------------------------------*/
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
   * Get the next ID from the table. Loops back to 0 when it hits the last ID.
   * @param table
   * @return the next ID
   */
  public String getNextKey(String table) {
    String nextKey = "";
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      nextKey += nextInsertDocKeyCounties;
      // increment
      nextInsertDocKeyCounties = (nextInsertDocKeyCounties + 1) % getTotalDocsCount(GEO_DOCUMENT_PREFIX_COUNTIES); 
      break;
    case GEO_DOCUMENT_PREFIX_ROUTES:
      nextKey += nextInsertDocKeyRoutes;
      nextInsertDocKeyRoutes = (nextInsertDocKeyRoutes + 1) % getTotalDocsCount(GEO_DOCUMENT_PREFIX_ROUTES);
      break;
    default:
      break;
    }
    return nextKey;
  }
  
  
  /**
   * Gets a document from memcached and prepares it to become the predicate of a GeoLoad.
   * @param table
   * @return the new doc body
   */
  public String buildGeoLoadDocument(String table, int key) {
    String storageKey = "";
    String keyPrefix = "";
    String docBody = null;
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      keyPrefix = GEO_DOCUMENT_PREFIX_COUNTIES + GEO_SYSTEMFIELD_DELIMITER;
      increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER, 1);
      break;
    case GEO_DOCUMENT_PREFIX_ROUTES:
      keyPrefix = GEO_DOCUMENT_PREFIX_ROUTES + GEO_SYSTEMFIELD_DELIMITER;
      increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER, 1);
      break;
    default:
      break;
    }
    
    // get the document
    while(docBody == null) {
      System.out.println("STALL: " + keyPrefix + key);
      docBody = getOriginalDocument(table, key+"");
    }
    
    // Synthesize data
    String newDocBody = synthesize(table, docBody);
    
    // there is a chance to add the geometry to memcached as a potential parameter:
    rollChanceToAddDocAsParameter(table, newDocBody);
    
    return newDocBody;
  }
  
  /**
   * Synthesize data by shifting geometry coordinates.
   * @param jsonString
   * @return
   */
  private String synthesize(String table, String jsonString) {
    JSONObject obj = new JSONObject(jsonString);
    String geometryField = "";
    String coordinatesField = "";
    String typeField = "";
    String geoJSONPolygon = "Polygon";
    String geoJSONMultiLineString = "MultiLineString";
    
    switch(table) {
    case GEO_DOCUMENT_PREFIX_COUNTIES:
      geometryField = GEO_FIELD_COUNTIES_GEOMETRY;
      typeField = GEO_FIELD_COUNTIES_GEOMETRY_OBJ_TYPE;
      coordinatesField = GEO_FIELD_COUNTIES_GEOMETRY_OBJ_COORDINATES;
      break;
    case GEO_DOCUMENT_PREFIX_ROUTES:
      geometryField = GEO_FIELD_ROUTES_GEOMETRY;
      typeField = GEO_FIELD_ROUTES_GEOMETRY_OBJ_TYPE;
      coordinatesField = GEO_FIELD_ROUTES_GEOMETRY_OBJ_COORDINATES;
      break;
    default:
      break;
    }
    // check if geometry field exists
    if(obj.has(geometryField) && !obj.isNull(geometryField)) {
      // get coordinates array
      JSONArray coordArr = obj.getJSONObject(geometryField).getJSONArray(coordinatesField);
      if(coordArr.length() > 0) {
        // array to store new shifted coordinates field
        JSONArray newCoordArr = null;
        
        // check type
        if(obj.getJSONObject(geometryField).getString(typeField).equals(geoJSONPolygon) ||            // Polygon or MultiLineString type
            obj.getJSONObject(geometryField).getString(typeField).equals(geoJSONMultiLineString)) { 
          newCoordArr = new JSONArray();
          
          for(int i = 0; i < coordArr.length(); i++) {
            // line wrapper array
            JSONArray tempLine = new JSONArray();
            
            // unwrap to get to coordinates
            JSONArray arrWithPoints = coordArr.getJSONArray(i);
            
            for(int j = 0; j < arrWithPoints.length(); j++) {
              // shift individual points
              JSONArray shiftedPoint = new JSONArray(shiftPoint(arrWithPoints.getJSONArray(j)));
              
              // store shifted points in line wrapper
              tempLine.put(shiftedPoint);
            }
            // store new line
            newCoordArr.put(tempLine);
          }
        } else {                                                                                      // Point type
          newCoordArr = new JSONArray(shiftPoint(coordArr));
        }
        // replace coordinates field with new shifted coordinates array
        obj.getJSONObject(geometryField).put(coordinatesField, newCoordArr);
      }
    }
    
    return obj.toString();
  }
  
  private double[] shiftPoint(JSONArray point) {
    double[] newPoint = {
        point.getDouble(0) - (GeoWorkload.LONG_OFFSET * synthesisOffsetCols),
        point.getDouble(1) - (GeoWorkload.LAT_OFFSET * synthesisOffsetRows)
    };
    return newPoint;
  }
  
  /*-----------------------------------------------------------*/
  /*----------------PARAMETER GENERATION LOGIC-----------------*/
  /*-----------------------------------------------------------*/

  public DataFilter getGeoPredicate() {
    return geoPredicate;
  }
  
  /**
   * Generates a random polygon geometry parameter from the counties table to parameterize intersection
   */
  public void buildGeoReadPredicate() {
    DataFilter result = new DataFilter();
    
    // nestedPredicateA.name = "geometry"
    DataFilter nestedA = new DataFilter();
    nestedA.setName(GEO_FIELD_COUNTIES_GEOMETRY);

    // fetch random geo predicate from memcached
    Random rand = new Random();
    int maxKey = getStoredGeoCount(GEO_DOCUMENT_PREFIX_COUNTIES);
    String geo = getGeometryDocument(GEO_DOCUMENT_PREFIX_COUNTIES, rand.nextInt(maxKey) + "");
   
    DataFilter nestedC = new DataFilter();
    nestedC.setValueA(new JSONObject(geo));
    
    result.setNestedPredicateA(nestedA);
    result.setNestedPredicateC(nestedC);
  }

  
}


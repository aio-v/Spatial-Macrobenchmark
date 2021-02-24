package site.ycsb.workloads.geo;

import site.ycsb.ByteIterator;
import site.ycsb.GeoDB;
import site.ycsb.Status;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.generator.geo.ParameterGenerator;
import site.ycsb.workloads.CoreWorkload;
import site.ycsb.generator.geo.MemcachedGenerator;
import site.ycsb.WorkloadException;

import java.util.HashMap;
import java.util.Properties;

/**
 * Author: Yuvraj Kanwar.
 */
public class GeoWorkload extends CoreWorkload {

  protected DiscreteGenerator operationchooser;
  

  public static final String STORAGE_HOST = "geo_storage_host";
  public static final String STORAGE_HOST_DEFAULT = "localhost";
  public static final String STORAGE_PORT = "geo_storage_port";
  public static final String STORAGE_PORT_DEFAULT = "11211";
  public static final String TOTAL_DOCS = "totalrecordcount";
  public static final String TOTAL_DOCS_DEFAULT = "118896";
  public static final String DOCS_START_VALUE = "1001";
  
  /* Additional tables required for macro-benchmarks */
  public static final String TABLE2NAME_PROPERTY = "table2";
  public static final String TABLE2NAME_PROPERTY_DEFAULT = "routes";
  public static final String TOTAL_DOCS_TABLE2 = "table2_totalrecordcount";
  public static final String TOTAL_DOCS_DEFAULT_TABLE2 = "26715";
  protected String table2;
  /*-------------------------------------------------*/

  public static final String RECORD_COUNT = "recordcount";
  public static final String RECORD_COUNT_DEFAULT = "1000000";

  public static final String GEO_INSERT_PROPORTION_PROPERTY = "geo_insert";
  public static final String GEO_INSERT_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_UPDATE_PROPORTION_PROPERTY = "geo_update";
  public static final String GEO_UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_NEAR_PROPORTION_PROPERTY = "geo_near";
  public static final String GEO_NEAR_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_BOX_PROPORTION_PROPERTY = "geo_box";
  public static final String GEO_BOX_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_INTERSECT_PROPORTION_PROPERTY = "geo_intersect";
  public static final String GEO_INTERSECT_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_SCAN_PROPORTION_PROPERTY = "geo_scan";
  public static final String GEO_SCAN_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_QUERY_LIMIT_MIN = "geo_querylimit_min";
  public static final String GEO_QUERY_LIMIT_MIN_DEFAULT = "10";
  public static final String GEO_QUERY_LIMIT_MAX = "geo_querylimit_max";
  public static final String GEO_QUERY_LIMIT_MAX_DEFAULT = "100";

  public static final String GEO_QUERY_OFFSET_MIN = "geo_offset_min";
  public static final String GEO_QUERY_OFFSET_MIN_DEFAULT = "10";
  public static final String GEO_QUERY_OFFSET_MAX = "geo_offset_max";
  public static final String GEO_QUERY_OFFSET_MAX_DEFAULT = "100";
  public static final String GEO_REQUEST_DISTRIBUTION = "geo_request_distribution";
  public static final String GEO_REQUEST_DISTRIBUTION_DEFAULT = "uniform";

  private static double recordCount = 1000000;
  
  /* Additional use case operations required for macro-benchmarks */
  public static final String GEO_CASE1_PROPERTY = "geo_case_graffiti_by_schools";
  public static final String GEO_CASE1_PROPERTY_DEFAULT = "0.00";
  
  public static final String GEO_CASE2_PROPERTY = "geo_case_graffiti_density";
  public static final String GEO_CASE2_PROPERTY_DEFAULT = "0.00";
  
  public static final String GEO_CASE3_PROPERTY = "geo_case_graffiti_by_high_traffic";
  public static final String GEO_CASE3_PROPERTY_DEFAULT = "0.00";
  public static final int TOP_CELL_COUNT = 5;
  
  public static final String GEO_CASE4_PROPERTY ="geo_case_clean_graffiti";
  public static final String GEO_CASE4_PROPERTY_DEFAULT = "0.00";
  
  public static final String GEO_CLEAN_BASED_ON_PROPERTY = "geo_clean_based_on";
  public static final String GEO_CLEAN_BASED_ON_PROPERTY_DEFAULT = "geo_case_graffiti_by_schools";
  private static String cleanBasedOn;
  
  public static final String DATA_SIZE = "datasize";
  /*--------------------------------------------*/
  
  /* Additional variables for synthesizing data */
  public static final double LAT_OFFSET = 14.5219043;
  public static final double LONG_OFFSET = 16.1346741;
  public static final double LAT_MIN = 31.0295791692;
  public static final double LAT_MAX = 45.5514834662;
  public static final double LONG_MIN = 129.408463169;
  public static final double LONG_MAX = 145.543137242;
  public static final int GRID_COLS = 6;
  public static final int GRID_ROWS = 10;
  /*--------------------------------------------*/
  
  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    recordCount = Double.parseDouble(
        p.getProperty(RECORD_COUNT, RECORD_COUNT_DEFAULT));
    String memHost = p.getProperty(STORAGE_HOST, STORAGE_HOST_DEFAULT);
    String memPort = p.getProperty(STORAGE_PORT, STORAGE_PORT_DEFAULT);
    String totalDocs = p.getProperty(TOTAL_DOCS, TOTAL_DOCS_DEFAULT);
    try {
      String table2temp = p.getProperty(TABLE2NAME_PROPERTY, null);
      // use additional tables
      if(table2temp != null) {
        table2 = table2temp;
        String totalDocs2 = p.getProperty(TOTAL_DOCS_TABLE2, TOTAL_DOCS_DEFAULT_TABLE2);
        MemcachedGenerator mcache = new MemcachedGenerator(p, memHost, memPort, totalDocs, totalDocs2);
        
        if(threadcount > 1) {
          System.out.println("\tTHREADID: " + mythreadid);
          for(int i = 0; i < mythreadid * (recordCount/threadcount); i++) {
            mcache.incrementSynthesisOffset();
          }
        }
        
        return mcache;
      } else { // use one table
        return new MemcachedGenerator(p, memHost, memPort, totalDocs);
      }
    } catch (Exception e) {
      System.err.println("Memcached generator init failed " + e.getMessage());
      throw new WorkloadException();
    }
  }

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    operationchooser = createOperationGenerator(p);
    recordCount = Double.parseDouble(
        p.getProperty(RECORD_COUNT, RECORD_COUNT_DEFAULT));
    cleanBasedOn = p.getProperty(GEO_CLEAN_BASED_ON_PROPERTY, GEO_CLEAN_BASED_ON_PROPERTY_DEFAULT);
  }

  @Override
  public boolean doInsert(GeoDB db, Object threadstate) {
	  //System.out.println("\n\n\n\n\n\n***** here is in load phase? " + recordCount);
    Status status;
    status = (table2 != null) ? 
        db.geoLoad(table, table2, (MemcachedGenerator) threadstate, recordCount)
        : db.geoLoad(table, (MemcachedGenerator) threadstate, recordCount);
    return null != status && status.isOk();
  }



  @Override
  public boolean doTransaction(GeoDB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }
    MemcachedGenerator  generator = (MemcachedGenerator) threadstate;
    System.out.println(operation);
    switch (operation) {
//    case "READ":
//      doTransactionRead(db);                      /*--------------------NOT USED--------------------*/
//      break;
//    case "UPDATE":
//      doTransactionUpdate(db);                    /*--------------------NOT USED--------------------*/
//      break;
//    case "INSERT":
//      doTransactionInsert(db);                    /*--------------------NOT USED--------------------*/
//      break;
//    case "GEO_INSERT":
//      doTransactionGeoInsert(db, generator);      /*--------------------NOT USED--------------------*/
//      break;
//    case "GEO_UPDATE":
//      doTransactionGeoUpdate(db, generator);      /*--------------------NOT USED--------------------*/
//      break;
//    case "GEO_NEAR":
//      doTransactionGeoNear(db, generator);        /*--------------------NOT USED--------------------*/
//      break;
//    case "GEO_BOX":
//      doTransactionGeoBox(db, generator);         /*--------------------NOT USED--------------------*/
//      break;
    case "GEO_INTERSECT":
      doTransactionGeoIntersect(db, generator);
      break;
//    case "GEO_SCAN":
//      doTransactionGeoScan(db, generator);        /*--------------------NOT USED--------------------*/
//      break;
//    case "GEO_CASE_GRAFFITI_BY_SCHOOLS":
//      doTransactionGeoCase1(db, generator);       /*--------------------NOT USED--------------------*/
//      break;
//    case "GEO_CASE_GRAFFITI_BY_DENSITY":
//      doTransactionGeoCase2(db, generator);       /*--------------------NOT USED--------------------*/
//      break;
//    case "GEO_CASE_GRAFFITI_BY_HIGH_TRAFFIC":
//      doTransactionGeoCase3(db, generator);       /*--------------------NOT USED--------------------*/
//      break;
//    case "GEO_CASE_CLEAN_GRAFFITI":
//      doTransactionGeoCase4(db, generator);       /*--------------------NOT USED--------------------*/
//      break;
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }


//  public void doTransactionGeoInsert(GeoDB db, ParameterGenerator generator) {
//    try {
//      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
//      db.geoInsert(table, cells, generator);
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      ex.printStackTrace(System.out);
//    }
//  }
//
//  public void doTransactionGeoUpdate(GeoDB db, ParameterGenerator generator) {
//    try {
//      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
//      db.geoUpdate(table, cells, generator);
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      ex.printStackTrace(System.out);
//    }
//  }
//
//  public void doTransactionGeoNear(GeoDB db, ParameterGenerator generator) {
//    try {
//      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
//      db.geoNear(table, cells, generator);
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      ex.printStackTrace(System.out);
//    }
//  }
//
//  public void doTransactionGeoBox(GeoDB db, ParameterGenerator generator) {
//    try {
//      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
//      db.geoBox(table, cells, generator);
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      ex.printStackTrace(System.out);
//    }
//  }

  public void doTransactionGeoIntersect(GeoDB db, ParameterGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.geoIntersect(table2, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

//  public void doTransactionGeoScan(GeoDB db, ParameterGenerator generator) {
//    try {
//      db.geoScan(table, new Vector<HashMap<String, ByteIterator>>(), generator);
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      ex.printStackTrace(System.out);
//    }
//  }
//  
//  /* USE CASE OPERATIONS */
//  public void doTransactionGeoCase1(GeoDB db, ParameterGenerator generator) {
//    HashMap<String, Vector<HashMap<String, ByteIterator>>> cells = new HashMap<>();
//    try {
//      db.geoUseCase1(table, cells, generator);
//      
//      // print result for confirmation
//      for(String school : cells.keySet()) {
//        System.out.println("\tGraffiti around " + school + ": " + cells.get(school).size());
//      }
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      ex.printStackTrace(System.out);
//    }
//  }
//  
//  public void doTransactionGeoCase2(GeoDB db, ParameterGenerator generator) {
//    HashMap<String, Vector<HashMap<String, ByteIterator>>> cells = new HashMap<>();
//    try {
//      db.geoUseCase2(table, cells, generator);
//      // print result for confirmation
//      for(String key : cells.keySet()) {
//        System.out.println("\t Graffiti in " + key + ": " + cells.get(key).size());
//      }
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      ex.printStackTrace(System.out);
//    }
//  }
//  
//  public void doTransactionGeoCase3(GeoDB db, ParameterGenerator generator) {
//    HashMap<String, Vector<HashMap<String, ByteIterator>>> cells = new HashMap<>();
//    try {
//      db.geoUseCase3(table3, table, cells, generator);
//      // print result for confirmation
//      
//      int counter = 1;
//      for(String key : cells.keySet()) {
//        System.out.println("Rank " + counter + " traffic: " + cells.get(key).size());
//        counter++;
//      }
//     
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      ex.printStackTrace(System.out);
//    }
//  }
//  
//  public void doTransactionGeoCase4(GeoDB db, ParameterGenerator generator) {
//    HashSet<Integer> deleted = new HashSet<Integer>();
//    try {
//      switch(cleanBasedOn) {
//      case GEO_CASE1_PROPERTY:
//        generator.buildGeoPredicateCase1();
//        break;
//      case GEO_CASE2_PROPERTY:
//        generator.buildGeoPredicateCase3();
//        break;
//      case GEO_CASE3_PROPERTY:
//        generator.buildGeoPredicateCase3();
//        break;
//      default:
//        break;
//      }
//      db.geoUseCase4(table, cleanBasedOn, deleted, generator);
//    } catch (Exception ex) {
//      ex.printStackTrace();
//    }
//  }


  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));


    final double geoInsert = Double.parseDouble(
        p.getProperty(GEO_INSERT_PROPORTION_PROPERTY, GEO_INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double geoUpdate = Double.parseDouble(
        p.getProperty(GEO_UPDATE_PROPORTION_PROPERTY, GEO_UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double geoNear = Double.parseDouble(
        p.getProperty(GEO_NEAR_PROPORTION_PROPERTY, GEO_NEAR_PROPORTION_PROPERTY_DEFAULT));
    final double geoBox = Double.parseDouble(
        p.getProperty(GEO_BOX_PROPORTION_PROPERTY, GEO_BOX_PROPORTION_PROPERTY_DEFAULT));
    final double geoIntersect = Double.parseDouble(
        p.getProperty(GEO_INTERSECT_PROPORTION_PROPERTY, GEO_INTERSECT_PROPORTION_PROPERTY_DEFAULT));
    final double geoScan = Double.parseDouble(
        p.getProperty(GEO_SCAN_PROPORTION_PROPERTY, GEO_SCAN_PROPORTION_PROPERTY_DEFAULT));
    
    
    final double geoCaseGraffitiBySchools = Double.parseDouble(
        p.getProperty(GEO_CASE1_PROPERTY, GEO_CASE1_PROPERTY_DEFAULT));
    final double geoCaseGraffitiDensity = Double.parseDouble(
        p.getProperty(GEO_CASE2_PROPERTY, GEO_CASE2_PROPERTY_DEFAULT));
    final double geoCaseGraffitiByBuildings = Double.parseDouble(
        p.getProperty(GEO_CASE3_PROPERTY, GEO_CASE3_PROPERTY_DEFAULT));
    final double geoCaseCleanGraffiti = Double.parseDouble(
        p.getProperty(GEO_CASE4_PROPERTY, GEO_CASE4_PROPERTY_DEFAULT));
    
    
    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (scanproportion > 0) {
      operationchooser.addValue(scanproportion, "SCAN");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }

    if (geoInsert > 0) {
      operationchooser.addValue(geoInsert, "GEO_INSERT");
    }

    if (geoUpdate > 0) {
      operationchooser.addValue(geoUpdate, "GEO_UPDATE");
    }

    if (geoNear > 0) {
      operationchooser.addValue(geoNear, "GEO_NEAR");
    }

    if (geoBox > 0) {
      operationchooser.addValue(geoBox, "GEO_BOX");
    }

    if (geoIntersect > 0) {
      operationchooser.addValue(geoIntersect, "GEO_INTERSECT");
    }

    if (geoScan > 0) {
      operationchooser.addValue(geoScan, "GEO_SCAN");
    }
    
    if (geoCaseGraffitiBySchools > 0) {
      operationchooser.addValue(geoCaseGraffitiBySchools, "GEO_CASE_GRAFFITI_BY_SCHOOLS");
    }
    
    if (geoCaseGraffitiDensity > 0) {
      operationchooser.addValue(geoCaseGraffitiDensity, "GEO_CASE_GRAFFITI_BY_DENSITY");
    }
    
    if (geoCaseGraffitiByBuildings > 0) {
      operationchooser.addValue(geoCaseGraffitiByBuildings, "GEO_CASE_GRAFFITI_BY_HIGH_TRAFFIC");
    }
    
    if (geoCaseCleanGraffiti > 0) {
      operationchooser.addValue(geoCaseCleanGraffiti, "GEO_CASE_CLEAN_GRAFFITI");
    }

    return operationchooser;
  }
  
  public static double getRecordCount() {
    return recordCount;
  }
}


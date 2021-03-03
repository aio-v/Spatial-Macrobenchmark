package site.ycsb;

import site.ycsb.generator.geo.ParameterGenerator;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Author: Yuvraj Kanwar
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 *
 * Note that YCSB does not make any use of the return codes returned by this class.
 * Instead, it keeps a count of the return values and presents them to the user.
 *
 * The semantics of methods such as insert, update and delete vary from database
 * to database.  In particular, operations may or may not be durable once these
 * methods commit, and some systems may return 'success' regardless of whetherF
 * or not a tuple with a matching key existed before the call.  Rather than dictate
 * the exact semantics of these methods, we recommend you either implement them
 * to match the database's default semantics, or the semantics of your
 * target application.  For the sake of comparison between experiments we also
 * recommend you explain the semantics you chose when presenting performance results.
 */
public abstract class GeoDB extends DB {
  /**
   * Properties for configuring this DB.
   */
  private Properties properties = new Properties();

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p) {
    properties = p;

  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  public abstract Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result);

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
   * in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  public abstract Status scan(String table, String startkey, int recordcount, Set<String> fields,
                              Vector<HashMap<String, ByteIterator>> result);

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  public abstract Status update(String table, String key, HashMap<String, ByteIterator> values);

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  public abstract Status insert(String table, String key, HashMap<String, ByteIterator> values);



  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  public abstract Status delete(String table, String key);


  /**
   *
   *  GEO operations.
   *
   */

  // overloading the standard "insert" operation as it used by YCSB for loading data
  public Status geoLoad(String table, ParameterGenerator generator, Double recordCount) {
    return null;
  }
  
  // a geoLoad for loading multiple tables
  public abstract Status geoLoad(String table1, String table2, ParameterGenerator generator, Double recordCount);

  public Status geoInsert(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen)  {
    System.err.println("geoInsert not implemented");
    return null;
  }
  
  // a geoInsert that works on multiple tables
  public Status geoInsert(String table, String value, ParameterGenerator gen) {
    return null;
  }

  public Status geoUpdate(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen)  {
    System.err.println("geoUpdate not implemented");
    return null;
  }

  public Status geoNear(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen)  {
    System.err.println("geoNear not implemented");
    return null;
  }

  public Status geoBox(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen)  {
    System.err.println("geoBox not implemented");
    return null;
  }

  public Status geoIntersect(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen)  {
    System.err.println("geoIntersect not implemented");
    return null;
  }
  
  public Status geoWithin(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen)  {
    System.err.println("geoIntersect not implemented");
    return null;
  }

  public Status geoScan(String table, Vector<HashMap<String, ByteIterator>> result, ParameterGenerator gen)  {
    System.err.println("geoScan not implemented");
    return null;
  }
  

  /* Use case operations for macro-benchmarks */
  /*--------------------NOT USED--------------------*/
//  public abstract Status geoUseCase1(String table, HashMap<String, 
//      Vector<HashMap<String, ByteIterator>>> result, ParameterGenerator gen);
//  
//  public abstract Status geoUseCase2(String table, HashMap<String, 
//      Vector<HashMap<String, ByteIterator>>> result, ParameterGenerator gen);
//  
//  public abstract Status geoUseCase3(String table1, String table2, HashMap<String,
//      Vector<HashMap<String, ByteIterator>>> result, ParameterGenerator gen);
//  
//  public abstract Status geoUseCase4(String table, String operation, Set<Integer> deleted, ParameterGenerator gen);

}

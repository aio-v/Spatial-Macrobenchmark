/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package com.yahoo.ycsb.db;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.GeoDB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.mongodb.util.JSON;

import com.yahoo.ycsb.generator.geo.ParameterGenerator;
import com.yahoo.ycsb.workloads.geo.DataFilter;
import com.yahoo.ycsb.workloads.geo.GeoWorkload;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends GeoDB {

  /** Used to include a field in a response. */
  private static final Integer INCLUDE = Integer.valueOf(1);

  /** The options to use for inserting many documents. */
  private static final InsertManyOptions INSERT_UNORDERED =
      new InsertManyOptions().ordered(false);

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions()
      .upsert(true);

  /**
   * The database name to access.
   */
  private static String databaseName;

  /** The database name to access. */
  private static MongoDatabase database;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** A singleton Mongo instance. */
  private static MongoClient mongoClient;

  /** The default read preference for the test. */
  private static ReadPreference readPreference;

  /** The default write concern for the test. */
  private static WriteConcern writeConcern;

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The bulk inserts pending for the thread. */
  private final List<Document> bulkInserts = new ArrayList<Document>();
  
  /** Hardcoded for use case 4, tester. */
  private ObjectId[] toDelete = {
      new ObjectId("5e5cce19267fe36af71b0d07"),
      new ObjectId("5e5cce19267fe36af71b0dc8"),
      new ObjectId("5e5cce19267fe36af71b0d2d"),
      new ObjectId("5e5cce18267fe36af71b0cdf"),
      new ObjectId("5e5cce19267fe36af71b0d50"),
      new ObjectId("5e5cce19267fe36af71b0d78"),
      new ObjectId("5e5cc52f267fe36af718e60d"),
      new ObjectId("5e5cc52e267fe36af718e5e1"),
      new ObjectId("5e5cce18267fe36af71b0bc9"),
      new ObjectId("5e5cce19267fe36af71b0d9f"),
      new ObjectId("5e5cc530267fe36af718e636"),
      new ObjectId("5e5cc53f267fe36af718e840"),
      new ObjectId("5e5ccef7267fe36af71ccd77"),
      new ObjectId("5e5ccecb267fe36af71c72dc"),
      new ObjectId("5e5ccc87267fe36af719d74f"),
      new ObjectId("5e5ccde6267fe36af71aa5ad"),
      new ObjectId("5e5ccdbf267fe36af71a5611"),
      new ObjectId("5e5cc924267fe36af7196abb"),
      new ObjectId("5e5cc98f267fe36af71978f8"),
      new ObjectId("5e5cce19267fe36af71b0ded"),
      new ObjectId("5e5cc990267fe36af7197910"),
      new ObjectId("5e5cc991267fe36af719793a"),
      new ObjectId("5e5cc541267fe36af718e89e"),
      new ObjectId("5e5ccd98267fe36af71a0556"),
      new ObjectId("5e5cce2d267fe36af71b371c"),
      new ObjectId("5e5ccf79267fe36af71dcca1"),
      new ObjectId("5e5ccfb7267fe36af71e4c2c"),
      new ObjectId("5e5ccf77267fe36af71dc9ed"),
      new ObjectId("5e5ccc72267fe36af719d4a2"),
      new ObjectId("5e5ccfa2267fe36af71e219f"),
      new ObjectId("5e5ccc71267fe36af719d478"),
      new ObjectId("5e5cd066267fe36af71faf98"),
      new ObjectId("5e5ccf78267fe36af71dca1c"),
      new ObjectId("5e5cd07b267fe36af71fda21"),
      new ObjectId("5e5cc540267fe36af718e86f"),
      new ObjectId("5e5cd07b267fe36af71fda41"),
      new ObjectId("5e5ccc1c267fe36af719c9f1"),
      new ObjectId("5e5ccc80267fe36af719d65f"),
      new ObjectId("5e5ccc7f267fe36af719d638"),
      new ObjectId("5e5cce18267fe36af71b0c64"),
      new ObjectId("5e5cce1a267fe36af71b1029"),
      new ObjectId("5e5cce18267fe36af71b0c3b"),
      new ObjectId("5e5cc531267fe36af718e666"),
      new ObjectId("5e5cbd6f267fe36af717c738"),
      new ObjectId("5e5cce18267fe36af71b0bee"),
      new ObjectId("5e5ccdd0267fe36af71a77fa"),
      new ObjectId("5e5cce8e267fe36af71bfdae"),
      new ObjectId("5e5ccf73267fe36af71dc163"),
      new ObjectId("5e5cceb1267fe36af71c44b6"),
      new ObjectId("5e5cce2e267fe36af71b3859"),
      new ObjectId("5e5cc02a267fe36af7183082"),
      new ObjectId("5e5ccf73267fe36af71dc13c"),
      new ObjectId("5e5cd07b267fe36af71fdaca"),
      new ObjectId("5e5ccdbf267fe36af71a563d"),
      new ObjectId("5e5cd07b267fe36af71fdaec"),
      new ObjectId("5e5cd07b267fe36af71fdaa7"),
      new ObjectId("5e5cc069267fe36af7183994"),
      new ObjectId("5e5cd07c267fe36af71fdb30"),
      new ObjectId("5e5cd07c267fe36af71fdb0e"),
      new ObjectId("5e5cce18267fe36af71b0c15"),
      new ObjectId("5e5ccfa8267fe36af71e2d98"),
      new ObjectId("5e5ccea3267fe36af71c276c"),
      new ObjectId("5e5ccfc1267fe36af71e608c"),
      new ObjectId("5e5cd07b267fe36af71fda86"),
      new ObjectId("5e5cd07b267fe36af71fda63"),
      new ObjectId("5e5ccf24267fe36af71d2155"),
      new ObjectId("5e5cce47267fe36af71b6d27"),
      new ObjectId("5e5ccfcb267fe36af71e7507"),
      new ObjectId("5e5cc0a4267fe36af718420e"),
      new ObjectId("5e5ccfc7267fe36af71e6c30"),
      new ObjectId("5e5cc7dc267fe36af719421c"),
      new ObjectId("5e5ccf24267fe36af71d2182"),
      new ObjectId("5e5cd068267fe36af71fb381"),
      new ObjectId("5e5ccf3f267fe36af71d5860"),
      new ObjectId("5e5ccf3f267fe36af71d57c4"),
      new ObjectId("5e5ccfcb267fe36af71e752f"),
      new ObjectId("5e5cc948267fe36af7196f7d"),
      new ObjectId("5e5cd05f267fe36af71fa121"),
      new ObjectId("5e5ccece267fe36af71c7948"),
      new ObjectId("5e5ccecb267fe36af71c72b2"),
      new ObjectId("5e5cbdd1267fe36af717d616"),
      new ObjectId("5e5cce47267fe36af71b6c1a"),
      new ObjectId("5e5ccf2a267fe36af71d2ce5"),
      new ObjectId("5e5cc497267fe36af718d11f"),
      new ObjectId("5e5cc7db267fe36af71941f0"),
      new ObjectId("5e5cce47267fe36af71b6bf1"),
      new ObjectId("5e5cce02267fe36af71adf3a"),
      new ObjectId("5e5cce02267fe36af71adf0f"),
      new ObjectId("5e5cce1a267fe36af71b1003"),
      new ObjectId("5e5cd071267fe36af71fc6f1"),
      new ObjectId("5e5ccece267fe36af71c7a63"),
      new ObjectId("5e5cce1a267fe36af71b0f8e"),
      new ObjectId("5e5ccfd0267fe36af71e7f8c"),
      new ObjectId("5e5cbe8e267fe36af717f311"),
      new ObjectId("5e5cc484267fe36af718ce5b"),
      new ObjectId("5e5cc1c1267fe36af7186988"),
      new ObjectId("5e5ccc84267fe36af719d6d2"),
      new ObjectId("5e5ccfd0267fe36af71e7f68"),
      new ObjectId("5e5cc3d8267fe36af718b645"),
      new ObjectId("5e5ccdea267fe36af71aaebc"),
      new ObjectId("5e5cbe8f267fe36af717f333"),
      new ObjectId("5e5cc162267fe36af7185c35"),
      new ObjectId("5e5cce01267fe36af71adceb"),
      new ObjectId("5e5cce01267fe36af71adcc2"),
      new ObjectId("5e5cbe80267fe36af717f0f3"),
      new ObjectId("5e5cce48267fe36af71b6d78"),
      new ObjectId("5e5ccece267fe36af71c7920"),
      new ObjectId("5e5ccc83267fe36af719d6bb"),
      new ObjectId("5e5ccdc3267fe36af71a5e7a"),
      new ObjectId("5e5cbe8f267fe36af717f340"),
      new ObjectId("5e5cbe7f267fe36af717f0c5"),
      new ObjectId("5e5cd051267fe36af71f8590"),
      new ObjectId("5e5ccfd0267fe36af71e7fb5"),
      new ObjectId("5e5cbe7f267fe36af717f0bb"),
      new ObjectId("5e5cca45267fe36af7198f59"),
      new ObjectId("5e5cbe9c267fe36af717f535"),
      new ObjectId("5e5cbe7e267fe36af717f08d"),
      new ObjectId("5e5ccecd267fe36af71c77bd"),
      new ObjectId("5e5cca47267fe36af7198f85"),
      new ObjectId("5e5cd07c267fe36af71fdb54"),
      new ObjectId("5e5cbe9a267fe36af717f50b"),
      new ObjectId("5e5ccf56267fe36af71d851a"),
      new ObjectId("5e5ccf2c267fe36af71d3189"),
      new ObjectId("5e5cc170267fe36af7185e31"),
      new ObjectId("5e5cce47267fe36af71b6c42"),
      new ObjectId("5e5cc075267fe36af7183b52"),
      new ObjectId("5e5cc346267fe36af718a152"),
      new ObjectId("5e5cbe99267fe36af717f4de"),
      new ObjectId("5e5cbed7267fe36af717fe7d"),
      new ObjectId("5e5cc744267fe36af7192d75"),
      new ObjectId("5e5cc16f267fe36af7185e02"),
      new ObjectId("5e5cd079267fe36af71fd5e7"),
      new ObjectId("5e5cc7ff267fe36af71946e6"),
      new ObjectId("5e5ccfd0267fe36af71e7f40"),
      new ObjectId("5e5cd079267fe36af71fd69c"),
      new ObjectId("5e5cc16d267fe36af7185dd2"),
      new ObjectId("5e5ccecd267fe36af71c76de"),
      new ObjectId("5e5cc160267fe36af7185c06"),
      new ObjectId("5e5ccdf3267fe36af71ac079"),
      new ObjectId("5e5ccb2a267fe36af719ab87"),
      new ObjectId("5e5ccfd6267fe36af71e8b48"),
      new ObjectId("5e5cd068267fe36af71fb32f"),
      new ObjectId("5e5ccfb8267fe36af71e4d8e"),
      new ObjectId("5e5cbe8d267fe36af717f2e1"),
      new ObjectId("5e5ccff0267fe36af71ec09c"),
      new ObjectId("5e5cbe6f267fe36af717ee47"),
      new ObjectId("5e5ccda3267fe36af71a1bb2"),
      new ObjectId("5e5cbe9c267fe36af717f548"),
      new ObjectId("5e5cd068267fe36af71fb357"),
      new ObjectId("5e5ccecf267fe36af71c7b92"),
      new ObjectId("5e5cce1a267fe36af71b0fdd"),
      new ObjectId("5e5ccdb6267fe36af71a440c"),
      new ObjectId("5e5ccf24267fe36af71d21a6"),
      new ObjectId("5e5ccc82267fe36af719d68b"),
      new ObjectId("5e5ccf56267fe36af71d84ec"),
      new ObjectId("5e5cc7fe267fe36af71946bf"),
      new ObjectId("5e5ccda3267fe36af71a1bd6"),
      new ObjectId("5e5cbe6e267fe36af717ee18"),
      new ObjectId("5e5ccc85267fe36af719d6ff"),
      new ObjectId("5e5ccfcb267fe36af71e74df"),
      new ObjectId("5e5cbe8c267fe36af717f2c9"),
      new ObjectId("5e5cd04d267fe36af71f7ca7"),
      new ObjectId("5e5cc00f267fe36af7182cca"),
      new ObjectId("5e5cc0a3267fe36af71841e0"),
      new ObjectId("5e5ccfcb267fe36af71e74b8"),
      new ObjectId("5e5ccecd267fe36af71c7809"),
      new ObjectId("5e5cd04d267fe36af71f7c83"),
      new ObjectId("5e5cbe28267fe36af717e36f"),
      new ObjectId("5e5cd017267fe36af71f103a"),
      new ObjectId("5e5cc0a2267fe36af71841b5"),
      new ObjectId("5e5ccf2a267fe36af71d2cc0"),
      new ObjectId("5e5cc5d8267fe36af718fcf1"),
      new ObjectId("5e5cce1a267fe36af71b10c6"),
      new ObjectId("5e5cd01f267fe36af71f2085"),
      new ObjectId("5e5ccfb8267fe36af71e4db7"),
      new ObjectId("5e5cce72267fe36af71bc35a"),
      new ObjectId("5e5ccfc1267fe36af71e60b8"),
      new ObjectId("5e5cd07c267fe36af71fdc13"),
      new ObjectId("5e5ccf79267fe36af71dccc9"),
      new ObjectId("5e5cd07c267fe36af71fdb76"),
      new ObjectId("5e5cce70267fe36af71bc087"),
      new ObjectId("5e5ccc86267fe36af719d726"),
      new ObjectId("5e5ccf7d267fe36af71dd4c7"),
      new ObjectId("5e5ccfe6267fe36af71eac48"),
      new ObjectId("5e5cbe74267fe36af717ef13"),
      new ObjectId("5e5cbe7d267fe36af717f07d"),
      new ObjectId("5e5cce02267fe36af71adec4"),
      new ObjectId("5e5cbe9d267fe36af717f571"),
      new ObjectId("5e5cbe73267fe36af717eee9"),
      new ObjectId("5e5cce02267fe36af71adeec"),
      new ObjectId("5e5ccece267fe36af71c7a38"),
      new ObjectId("5e5ccfb8267fe36af71e4d6f"),
      new ObjectId("5e5cbf5b267fe36af71812f1"),
      new ObjectId("5e5cbf8a267fe36af71819ed"),
      new ObjectId("5e5ccfb8267fe36af71e4de1"),
      new ObjectId("5e5cce14267fe36af71b03ce"),
      new ObjectId("5e5ccfcf267fe36af71e7d0c"),
      new ObjectId("5e5ccdb0267fe36af71a37bd"),
      new ObjectId("5e5ccf2c267fe36af71d31b2"),
      new ObjectId("5e5cce71267fe36af71bc215"),
      new ObjectId("5e5ccfb8267fe36af71e4d44"),
      new ObjectId("5e5cbe7c267fe36af717f051"),
      new ObjectId("5e5ccfe4267fe36af71ea6da"),
      new ObjectId("5e5cc746267fe36af7192dc6"),
      new ObjectId("5e5ccfb7267fe36af71e4d19"),
      new ObjectId("5e5ccf55267fe36af71d846f"),
      new ObjectId("5e5ccdf7267fe36af71ac897"),
      new ObjectId("5e5cceec267fe36af71cb683"),
      new ObjectId("5e5cc297267fe36af718881d"),
      new ObjectId("5e5cce05267fe36af71ae43a"),
      new ObjectId("5e5ccf09267fe36af71ced77"),
      new ObjectId("5e5cce9a267fe36af71c14b5"),
      new ObjectId("5e5cc1ca267fe36af7186ac2"),
      new ObjectId("5e5cbef1267fe36af7180293"),
      new ObjectId("5e5ccfb7267fe36af71e4cf3"),
      new ObjectId("5e5cce36267fe36af71b48fc"),
      new ObjectId("5e5cbe90267fe36af717f36d"),
      new ObjectId("5e5cc09d267fe36af7184109"),
      new ObjectId("5e5cbe7b267fe36af717f021"),
      new ObjectId("5e5cce06267fe36af71ae7a1"),
      new ObjectId("5e5cce9a267fe36af71c159d"),
      new ObjectId("5e5cbe6d267fe36af717ede8"),
      new ObjectId("5e5ccf09267fe36af71ced4f"),
      new ObjectId("5e5cced0267fe36af71c7d94"),
      new ObjectId("5e5cce48267fe36af71b6d52"),
      new ObjectId("5e5cd05b267fe36af71f9965"),
      new ObjectId("5e5cce01267fe36af71adc71"),
      new ObjectId("5e5cce01267fe36af71adc9a"),
      new ObjectId("5e5cc853267fe36af7194f11"),
      new ObjectId("5e5cd01f267fe36af71f1fc0"),
      new ObjectId("5e5cc299267fe36af7188849"),
      new ObjectId("5e5cca5b267fe36af7199218"),
      new ObjectId("5e5cd027267fe36af71f2f37"),
      new ObjectId("5e5cc35a267fe36af718a423"),
      new ObjectId("5e5cce51267fe36af71b80be"),
      new ObjectId("5e5cbe8b267fe36af717f296"),
      new ObjectId("5e5cbe9e267fe36af717f5a6"),
      new ObjectId("5e5ccfb7267fe36af71e4ca4"),
      new ObjectId("5e5cca48267fe36af7198fb0"),
      new ObjectId("5e5cceeb267fe36af71cb43c"),
      new ObjectId("5e5cca4b267fe36af7199013"),
      new ObjectId("5e5ccfb8267fe36af71e4e0a"),
      new ObjectId("5e5cc410267fe36af718bdfe"),
      new ObjectId("5e5ccde6267fe36af71aa5d6"),
      new ObjectId("5e5ccf2b267fe36af71d300a"),
      new ObjectId("5e5cc978267fe36af71975f8"),
      new ObjectId("5e5ccf2b267fe36af71d2fdd"),
      new ObjectId("5e5cce06267fe36af71ae7c8"),
      new ObjectId("5e5cbe8a267fe36af717f266"),
      new ObjectId("5e5cd04c267fe36af71f7b08"),
      new ObjectId("5e5ccfb7267fe36af71e4cd0"),
      new ObjectId("5e5ccfb7267fe36af71e4c7e"),
      new ObjectId("5e5ccfe4267fe36af71ea6b4"),
      new ObjectId("5e5cca49267fe36af7198fe0"),
      new ObjectId("5e5ccf2b267fe36af71d302f"),
      new ObjectId("5e5cd07f267fe36af71fdfa4"),
      new ObjectId("5e5cbea0267fe36af717f5d8"),
      new ObjectId("5e5cd04c267fe36af71f7ae1"),
      new ObjectId("5e5cd04a267fe36af71f764b"),
      new ObjectId("5e5ccda5267fe36af71a2104"),
      new ObjectId("5e5ccf2b267fe36af71d3054"),
      new ObjectId("5e5cd04c267fe36af71f7b30"),
      new ObjectId("5e5cd068267fe36af71fb3a5"),
      new ObjectId("5e5ccf2b267fe36af71d307e"),
      new ObjectId("5e5cc857267fe36af7194fa8"),
      new ObjectId("5e5cc1dc267fe36af7186d56"),
      new ObjectId("5e5cd04c267fe36af71f7abf"),
      new ObjectId("5e5ccf2c267fe36af71d30a6"),
      new ObjectId("5e5cce1a267fe36af71b0fba"),
      new ObjectId("5e5cd067267fe36af71fb12a"),
      new ObjectId("5e5ccf7b267fe36af71dd1fb"),
      new ObjectId("5e5cd04c267fe36af71f7b56"),
      new ObjectId("5e5cbe7a267fe36af717eff1"),
      new ObjectId("5e5cced0267fe36af71c7d6a"),
      new ObjectId("5e5cc1dd267fe36af7186d83"),
      new ObjectId("5e5cc57f267fe36af718f121"),
      new ObjectId("5e5ccfda267fe36af71e931d"),
      new ObjectId("5e5ccf4d267fe36af71d72ec"),
      new ObjectId("5e5ccdc8267fe36af71a6824"),
      new ObjectId("5e5ccee6267fe36af71caa40"),
      new ObjectId("5e5ccf2c267fe36af71d30cc"),
      new ObjectId("5e5cce6f267fe36af71bbe75"),
      new ObjectId("5e5ccfda267fe36af71e92f5"),
      new ObjectId("5e5ccecd267fe36af71c7832"),
      new ObjectId("5e5ccf2c267fe36af71d3161"),
      new ObjectId("5e5cbe5f267fe36af717ebcb"),
      new ObjectId("5e5ccfda267fe36af71e92cf"),
      new ObjectId("5e5ccf2c267fe36af71d313d"),
      new ObjectId("5e5cd04c267fe36af71f7b7f"),
      new ObjectId("5e5cd05f267fe36af71fa14d"),
      new ObjectId("5e5ccf2c267fe36af71d3114"),
      new ObjectId("5e5cd04c267fe36af71f7bab"),
      new ObjectId("5e5cd017267fe36af71f0fc2"),
      new ObjectId("5e5cc1df267fe36af7186daa"),
      new ObjectId("5e5cd04c267fe36af71f7bd4"),
      new ObjectId("5e5ccf2c267fe36af71d30f5"),
      new ObjectId("5e5cbe6a267fe36af717ed89"),
      new ObjectId("5e5cd002267fe36af71ee4ca"),
      new ObjectId("5e5ccfda267fe36af71e92a7"),
      new ObjectId("5e5cc074267fe36af7183b2c"),
      new ObjectId("5e5cc64e267fe36af7190c9b"),
      new ObjectId("5e5cc526267fe36af718e4c3"),
      new ObjectId("5e5ccfda267fe36af71e9283"),
      new ObjectId("5e5cd04c267fe36af71f7bfd"),
      new ObjectId("5e5cc952267fe36af71970d0"),
      new ObjectId("5e5cce1a267fe36af71b109e"),
      new ObjectId("5e5cbe89267fe36af717f23e"),
      new ObjectId("5e5cbe61267fe36af717ec2a"),
      new ObjectId("5e5cd04d267fe36af71f7c26"),
      new ObjectId("5e5cc1c0267fe36af7186951"),
      new ObjectId("5e5cd04d267fe36af71f7c5c"),
      new ObjectId("5e5cce1a267fe36af71b10f1"),
      new ObjectId("5e5cc3dc267fe36af718b6c8"),
      new ObjectId("5e5ccfd9267fe36af71e925a"),
      new ObjectId("5e5ccf4d267fe36af71d72c7"),
      new ObjectId("5e5cd017267fe36af71f0fe9"),
      new ObjectId("5e5cc1e0267fe36af7186dd8"),
      new ObjectId("5e5ccfd9267fe36af71e9231"),
      new ObjectId("5e5cc539267fe36af718e78a"),
      new ObjectId("5e5cc580267fe36af718f155"),
      new ObjectId("5e5ccfd9267fe36af71e9209"),
      new ObjectId("5e5cc052267fe36af7183645"),
      new ObjectId("5e5cbe79267fe36af717efca"),
      new ObjectId("5e5cce1a267fe36af71b107b"),
      new ObjectId("5e5ccdf6267fe36af71ac70c"),
      new ObjectId("5e5cc527267fe36af718e4ee"),
      new ObjectId("5e5cc538267fe36af718e762"),
      new ObjectId("5e5cc53b267fe36af718e7bd"),
      new ObjectId("5e5cc53c267fe36af718e7e8"),
      new ObjectId("5e5cc062267fe36af718389e"),
      new ObjectId("5e5cc859267fe36af7194ffd"),
      new ObjectId("5e5cbed6267fe36af717fe51"),
      new ObjectId("5e5ccc7c267fe36af719d5e5"),
      new ObjectId("5e5ccc7d267fe36af719d60c"),
      new ObjectId("5e5cc1e0267fe36af7186de6"),
      new ObjectId("5e5cd05b267fe36af71f99e1"),
      new ObjectId("5e5ccfd9267fe36af71e91e1"),
      new ObjectId("5e5cce04267fe36af71ae3ea"),
      new ObjectId("5e5cd05b267fe36af71f99b9"),
      new ObjectId("5e5ccebe267fe36af71c5965"),
      new ObjectId("5e5cbe5d267fe36af717eb98"),
      new ObjectId("5e5cceea267fe36af71cb34d"),
      new ObjectId("5e5cc53d267fe36af718e814"),
      new ObjectId("5e5ccdc3267fe36af71a5ea2"),
      new ObjectId("5e5cbf59267fe36af71812bd"),
      new ObjectId("5e5ccfcb267fe36af71e75af"),
      new ObjectId("5e5ccfd9267fe36af71e91b9"),
      new ObjectId("5e5cceb5267fe36af71c4b6e"),
      new ObjectId("5e5cc584267fe36af718f1e3"),
      new ObjectId("5e5cc537267fe36af718e733"),
      new ObjectId("5e5ccf26267fe36af71d2516"),
      new ObjectId("5e5cc163267fe36af7185c5f"),
      new ObjectId("5e5cd05b267fe36af71f998f"),
      new ObjectId("5e5cc1c2267fe36af71869ac"),
      new ObjectId("5e5cce18267fe36af71b0c8e"),
      new ObjectId("5e5cc581267fe36af718f17e"),
      new ObjectId("5e5ccee7267fe36af71cac2a"),
      new ObjectId("5e5ccfd9267fe36af71e916a"),
      new ObjectId("5e5ccfd9267fe36af71e9191"),
      new ObjectId("5e5cc40d267fe36af718bd96"),
      new ObjectId("5e5ccee7267fe36af71cad94"),
      new ObjectId("5e5cceec267fe36af71cb721"),
      new ObjectId("5e5cd020267fe36af71f21c0"),
      new ObjectId("5e5cced0267fe36af71c7db8")
  };

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (Exception e1) {
        System.err.println("Could not close MongoDB connection pool: "
            + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        database = null;
        mongoClient = null;
      }
    }
  }

   /*
       ================    GEO operations  ======================
   */

  @Override
  public Status geoLoad(String table, ParameterGenerator generator, Double recordCount) {

    try {
      String key = generator.getIncidentsIdRandom();
      MongoCollection<Document> collection = database.getCollection(table);
      Random rand = new Random();
      int objId = rand.nextInt((Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT) -
          Integer.parseInt(GeoWorkload.DOCS_START_VALUE)) + 1)+Integer.parseInt(GeoWorkload.DOCS_START_VALUE);
      Document query = new Document("properties.OBJECTID", objId);
      FindIterable<Document> findIterable = collection.find(query);
      Document queryResult = findIterable.first();
      if (queryResult == null) {
        System.out.println(table+" ++++ "+collection);
        System.out.println(query);
        System.out.println("Empty return");

        return Status.OK;
      }

      generator.putIncidentsDocument(key, queryResult.toJson());
      System.out.println("Key : " + key + " Query Result :" + queryResult.toJson());
      generator.buildGeoInsertDocument();
      int inserts = (int) Math.round(recordCount/Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT))-1;
      for (double i = inserts; i > 0; i--) {
        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
        geoInsert(table, cells, generator);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
    }
    return Status.ERROR;
  }
  
  /** 
   * A geoLoad that loads ENTIRE multiple tables.
   * @param table1
   * @param table2
   * @param table3
   * @param generator
   * @param recordCount the duplication factor (total records will be tableXtotaldoccount*recordCount)
   */
  @Override
  public Status geoLoad(String table1, String table2, String table3, ParameterGenerator generator, Double recordCount) {
    try {
      if(geoLoad(table1, generator) == Status.ERROR) {
        return Status.ERROR;
      }
//      if(geoLoad(table2, generator) == Status.ERROR) {
//        return Status.ERROR;
//      }
//      if(geoLoad(table3, generator) == Status.ERROR) {
//        return Status.ERROR;
//      }
      generator.incrementSynthesisOffset();
      
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
    }
    return Status.ERROR;
  }
  
  /**
   * Private helper method to load ALL DOCS of a generic table.
   * @param table
   * @param generator
   * @return Status
   */
  private Status geoLoad(String table, ParameterGenerator generator) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      
      // Load EVERY document of the collection
      for(int i = 0; i < generator.getTotalDocsCount(table); i++) {
        // Get the next document
        String nextDocObjId = generator.getNextId(table);
        

        // Query database for the document
        Document query = new Document("properties.OBJECTID", Integer.parseInt(nextDocObjId));
        FindIterable<Document> findIterable = collection.find(query);
        Document queryResult = findIterable.first();
        if (queryResult == null) {
          if(table.equals(ParameterGenerator.GEO_DOCUMENT_PREFIX_BUILDINGS)) {
            continue;
          } else {
            System.out.println(table + " ++++ " + collection.toString());
            System.out.println(query);
            System.out.println("Empty return");

            return Status.OK;
          }
        }
        
        // Load the document to memcached, only ONCE --> if we are on the first iteration of loading
        if(generator.getSynthesisOffsetCols() == 1 && generator.getSynthesisOffsetRows() == 0) {
          generator.putDocument(table, nextDocObjId, queryResult.toJson());
          System.out.println("Key : " + nextDocObjId + " Query Result : " + queryResult.toJson());
        }
        
        // Synthesize new document
        ObjectId generatedId = new ObjectId();
        String newDocBody = generator.buildGeoInsertDocument(table, 
            Integer.parseInt(nextDocObjId), generatedId.toHexString());
        // Add to database
        geoInsert(table, newDocBody, generator);
        

        // If schools table, also add synthesized doc to memcached 
        if(table.equals(ParameterGenerator.GEO_DOCUMENT_PREFIX_SCHOOLS)) {         
          int newKey = Integer.parseInt(nextDocObjId) + (generator.getTotalDocsCount(table) * 
              ((generator.getSynthesisOffsetRows() * ParameterGenerator.getSynthesisOffsetMax()) 
                  + generator.getSynthesisOffsetCols()));
          generator.putDocument(table, newKey + "", newDocBody);
        }
      }
      
      return Status.OK;
      
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.toString());

      return Status.ERROR;
    }
  }
  

  // *********************  GEO Insert ********************************

  @Override
  public Status geoInsert(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen)  {

    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String key = gen.getGeoPredicate().getDocid();
      String value = gen.getGeoPredicate().getValue();
      Document toInsert = new Document("OBJECTID", key);
      DBObject body = (DBObject) JSON.parse(value);
      toInsert.put(key, body);
      System.out.println("NEW DOC: " + toInsert.toJson() + "\n");
      collection.insertOne(toInsert);

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  /* A modified geoInsert to work with geoLoad that loads multiple tables. */
  public Status geoInsert(String table, String value, ParameterGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
//      String value = gen.getGeoPredicate().getValue();
      System.out.println("Synthesis Offset Column: " + gen.getSynthesisOffsetCols() +
          "\tSynthesis Offset Row: " + gen.getSynthesisOffsetRows());
      System.out.println("NEW DOC: " + value + "\n");
      Document toInsert = Document.parse(value);
      collection.insertOne(toInsert);

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  // *********************  GEO Update ********************************

  @Override
  public Status geoUpdate(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Random rand = new Random();
      int key = rand.nextInt((Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT) -
          Integer.parseInt(GeoWorkload.DOCS_START_VALUE)) + 1)+Integer.parseInt(GeoWorkload.DOCS_START_VALUE);
      String updateFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject updateFieldValue = gen.getGeoPredicate().getNestedPredicateA().getValueA();

      HashMap<String, Object> updateFields = new ObjectMapper().readValue(updateFieldValue.toString(), HashMap.class);
      Document refPoint = new Document(updateFields);
      Document query = new Document().append("properties.OBJECTID", key);
      Document fieldsToSet = new Document();

      fieldsToSet.put(updateFieldName, refPoint);
      Document update = new Document("$set", fieldsToSet);

      UpdateResult res = collection.updateMany(query, update);
      if (res.wasAcknowledged() && res.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  // *********************  GEO Near ********************************

  @Override
  public Status geoNear(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String nearFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject nearFieldValue = gen.getGeoPredicate().getNestedPredicateA().getValueA();

      HashMap<String, Object> nearFields = new ObjectMapper().readValue(nearFieldValue.toString(), HashMap.class);
      Document refPoint = new Document(nearFields);
     // Document query = new Document("properties.OBJECTID", key);

      //FindIterable<Document> findIterable = collection.find(query);

      FindIterable<Document> findIterable = collection.find(Filters.near(
          nearFieldName, refPoint, 1000.0, 0.0));
      Document projection = new Document();
      for (String field : gen.getAllGeoFields().get(table)) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        geoFillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  // *********************  GEO Box ********************************

  @Override
  public Status geoBox(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String boxFieldName1 = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject boxFieldValue1 = gen.getGeoPredicate().getNestedPredicateA().getValueA();
      JSONObject boxFieldValue2 = gen.getGeoPredicate().getNestedPredicateB().getValueA();

      HashMap<String, Object> boxFields = new ObjectMapper().readValue(boxFieldValue1.toString(), HashMap.class);
      Document refPoint = new Document();
      refPoint.putAll(boxFields);
      HashMap<String, Object> boxFields1 = new ObjectMapper().readValue(boxFieldValue2.toString(), HashMap.class);
      Document refPoint2 = new Document();
      refPoint2.putAll(boxFields1);
      ArrayList coords1 = ((ArrayList) refPoint.get("coordinates"));
      List<Double> rp = new ArrayList<>();
      for(Object element: coords1) {
        rp.add((Double) element);
      }
      ArrayList coords2 = ((ArrayList) refPoint2.get("coordinates"));
      for(Object element: coords2) {
        rp.add((Double) element);
      }
      // Document query = new Document("properties.OBJECTID", key);

      //FindIterable<Document> findIterable = collection.find(query);

      FindIterable<Document> findIterable = collection.find(
          Filters.geoWithinBox(boxFieldName1, rp.get(0), rp.get(1), rp.get(2), rp.get(3)));
      Document projection = new Document();
      for (String field : gen.getAllGeoFields().get(table)) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        geoFillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  // *********************  GEO Intersect ********************************

  @Override
  public Status geoIntersect(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String fieldName1 = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject intersectFieldValue2 = gen.getGeoPredicate().getNestedPredicateC().getValueA();

      HashMap<String, Object> intersectFields = new ObjectMapper()
          .readValue(intersectFieldValue2.toString(), HashMap.class);
      Document refPoint = new Document(intersectFields);
      FindIterable<Document> findIterable = collection.find(
          Filters.geoIntersects(fieldName1, refPoint));
      Document projection = new Document();
      for (String field : gen.getAllGeoFields().get(table)) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        geoFillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  // *********************  GEO Scan ********************************
  @Override
  public Status geoScan(String table, final Vector<HashMap<String, ByteIterator>> result, ParameterGenerator gen) {
    String startkey = gen.getIncidentIdWithDistribution();
    int recordcount = gen.getRandomLimit();
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("OBJECTID", scanRange);

      FindIterable<Document> findIterable =
          collection.find(query).limit(recordcount);

      Document projection = new Document();
      for (String field : gen.getAllGeoFields().get(table)) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        geoFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }
  
  //*********************  GEO USE CASE 1 ********************************
  public Status geoUseCase1(String table, 
      final HashMap<String, Vector<HashMap<String, ByteIterator>>> result, ParameterGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      MongoCursor<Document> cursor = null;
      
      int maxGraffitiCount = Integer.MIN_VALUE;
      String maxGraffitiSchool = "";
      Vector<HashMap<String, ByteIterator>> maxGraffiti = null;
      
      // Perform near query on incidents for all school documents
      for(DataFilter school : gen.getGeometryPredicatesList()) {
        String nearFieldName = school.getNestedPredicateA().getName();
        HashMap<String, Object> nearFields = new ObjectMapper().readValue(
            school.getNestedPredicateA().getValueA().toString(), HashMap.class);
        Document refPoint = new Document(nearFields);
        
        // Query
        FindIterable<Document> findIterable = collection.find(Filters.near(
            nearFieldName, refPoint, 500.0, 0.0));
        
        // Get all query result's document fields
        Document projection = new Document();
        for (String field : gen.getAllGeoFields().get(table)) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
        
        // Add to result
        cursor = findIterable.iterator();      
        // If no graffiti was found near the school, the school gets an empty vector
//        if(!cursor.hasNext()) {
//          result.put(school.getName(), new Vector<HashMap<String, ByteIterator>>s());
//          continue;
//        }
        
        // If there is graffiti, add the results under the school's name
        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
        while (cursor.hasNext()) {
          HashMap<String, ByteIterator> resultMap =
              new HashMap<String, ByteIterator>();

          Document obj = cursor.next();
          geoFillMap(resultMap, obj);
          graffitiResults.add(resultMap);
        }
        if(graffitiResults.size() > maxGraffitiCount) {
          maxGraffitiSchool = school.getName() + school.getNestedPredicateA().getValueA().toString();
          maxGraffiti = graffitiResults;
          maxGraffitiCount = maxGraffiti.size();

          System.out.println(maxGraffitiSchool + " graffiti count: " + maxGraffiti.size());
        } else {
          System.out.println("PASS...");
        }
//        result.put(school.getName(), graffitiResults);
      }
      
      if(maxGraffiti == null) {
        return Status.ERROR;
      }
      result.put(maxGraffitiSchool, maxGraffiti);
      return Status.OK;
      
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  //*********************  GEO USE CASE 2 ********************************
  public Status geoUseCase2(String table, 
      final HashMap<String, Vector<HashMap<String, ByteIterator>>> result, ParameterGenerator gen) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      // Loop through grid of city
      for(DataFilter cell : gen.getGeometryPredicatesList()) {
        String fieldName = cell.getName();
        JSONObject intersectFieldValue = cell.getValueA();
        HashMap<String, Object> intersectFields = new ObjectMapper()
            .readValue(intersectFieldValue.toString(), HashMap.class);
        Document refPoint = new Document(intersectFields);

        // Query
        FindIterable<Document> findIterable = collection.find(
            Filters.geoWithin("geometry", refPoint));

        // Get all query result's document fields
        Document projection = new Document();
        for (String field : gen.getAllGeoFields().get(table)) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);

        // Add to result
        cursor = findIterable.iterator();

        if(!cursor.hasNext()) {
          result.put(intersectFieldValue.toString(), new Vector<HashMap<String, ByteIterator>>());
          System.out.println("No graffiti in this cell " + intersectFieldValue.toString());
          continue;
        }

        // If there is graffiti, add the results under the cell's locations
        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
        while (cursor.hasNext()) {
          HashMap<String, ByteIterator> resultMap =
              new HashMap<String, ByteIterator>();

          Document obj = cursor.next();
          geoFillMap(resultMap, obj);
          graffitiResults.add(resultMap);
        }
        result.put(intersectFieldValue.toString(), graffitiResults);
        System.out.println(intersectFieldValue.toString() + ": COUNT = " + graffitiResults.size());
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  //*********************  GEO USE CASE 3 ********************************
  public Status geoUseCase3(String table1, String table2, 
      final HashMap<String, Vector<HashMap<String, ByteIterator>>> result, ParameterGenerator gen) {    
    try {
      // Get density of BUILDINGS in grid cells (sum of areas)
      MongoCollection<Document> collection = database.getCollection(table1);
      MongoCursor<Document> cursor = null;
      
      // Contains densities per grid cell
      HashMap<JSONObject, Double> densities = new HashMap<>();
      
      // Loop through grid of city
      for(DataFilter cell : gen.getGeometryPredicatesList()) {
        String fieldName = cell.getName();
        JSONObject intersectFieldValue = cell.getValueA();
        HashMap<String, Object> intersectFields = new ObjectMapper()
            .readValue(intersectFieldValue.toString(), HashMap.class);
        Document refPoint = new Document(intersectFields);
        
        // Query
        FindIterable<Document> findIterable = collection.find(
            Filters.geoIntersects(fieldName, refPoint));
        
        // Project
        Document projection = new Document();
        for (String field : gen.getAllGeoFields().get(table1)) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
        
        cursor = findIterable.iterator();
        
        // Key of the grid cell = intersectFieldValue
        // If no buildings in the cell, put value to 0
        if(!cursor.hasNext()) {
          densities.put(intersectFieldValue, 0.0);
          continue;
        }
        
        // If there are buildings, sum their areas
        double density = 0;
        while (cursor.hasNext()) {
          Document obj = cursor.next();
          JSONObject jobj = new JSONObject(obj.toJson());
          Double area = ((JSONObject) jobj.get("properties")).getDouble(gen.getBuildingsShapeArea());
          density += area;
        }
        densities.put(intersectFieldValue, density);
        System.out.println("Cell: " + intersectFieldValue.toString() + " Density: " + density);
      }
      
      // Sort densities by value in descending order --> take the top HIGH_TRAFFIC_CELL_COUNT
      ArrayList<Entry<JSONObject, Double>> sortedDensities = new ArrayList<>(densities.entrySet());
      Collections.sort(sortedDensities, Collections.reverseOrder(new Comparator<Entry<JSONObject, Double>>() {
        @Override
        public int compare(Entry<JSONObject, Double> o1, Entry<JSONObject, Double> o2) {
          return o1.getValue().compareTo(o2.getValue());
        }
      }));
      
      // Find graffiti in the top HIGH_TRAFFIC_CELL_COUNT cells
      collection = database.getCollection(table2);
      cursor = null;
      for(int i = 0; i < GeoWorkload.TOP_CELL_COUNT; i++) {
        String intersectFieldValue = sortedDensities.get(i).getKey().toString();
        HashMap<String, Object> intersectFields = new ObjectMapper()
            .readValue(intersectFieldValue, HashMap.class);
        Document refPoint = new Document(intersectFields);
        
        // Query
        FindIterable<Document> findIterable = collection.find(
            Filters.geoIntersects("geometry", refPoint));
        
        // Project
        Document projection = new Document();
        for (String field : gen.getAllGeoFields().get(table2)) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
        
        // Add to results
        cursor = findIterable.iterator();
        
        // Key = predicate
        if(!cursor.hasNext()) {
          result.put(intersectFieldValue, new Vector<HashMap<String, ByteIterator>>());
          continue;
        }
        
        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
        while (cursor.hasNext()) {
          HashMap<String, ByteIterator> resultMap =
              new HashMap<String, ByteIterator>();

          Document obj = cursor.next();
          geoFillMap(resultMap, obj);
          graffitiResults.add(resultMap);
        }
        result.put(intersectFieldValue, graffitiResults);
        System.out.println("Cell: " + intersectFieldValue.toString() + " Graffiti count: " + graffitiResults.size());
      }

      return Status.OK;
    
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  //*********************  GEO USE CASE 4 ********************************
  public Status geoUseCase4(String table, String operation, Set<Integer> deleted, ParameterGenerator gen) {
//    HashMap<String, Vector<HashMap<String, ByteIterator>>> toDelete = new HashMap<>();
    try {
//      Status queryStatus = null;
//      // Based on the operation, clean all the graffiti resulting from that search
//      switch(operation) {
//      case "geo_case_graffiti_by_schools":
//        queryStatus = geoUseCase1(table, toDelete, gen);
//        break;
//      default:
//        return Status.ERROR;
//      }
//      
//      if(queryStatus == Status.ERROR) {
//        return Status.ERROR;
//      }
//      
      MongoCollection<Document> collection = database.getCollection(table);
//      
      int counter = 0;
      // delete from a query results list
//      for(String key : toDelete.keySet()) {
//        System.out.println("Graffiti in " + key + ": " + toDelete.get(key).size());
//        for(HashMap<String, ByteIterator> doc : toDelete.get(key)) {
//          System.out.println(doc.get("_id").toString());
//          //delete
//          BasicDBObject delete = new BasicDBObject();
//          delete.put("_id", new ObjectId(doc.get("_id").toString()));
//          counter += collection.deleteOne(delete).getDeletedCount();
//        }
//      }
//      System.out.println("\tDeleted: " + counter);
      
      // hardcoded value deletion
      for(ObjectId id : toDelete) { 
        BasicDBObject delete = new BasicDBObject();
        delete.put("_id", id);
        counter += collection.deleteOne(delete).getDeletedCount();
      }
      System.out.println("\tDeleted: " + counter);

      return Status.OK;

    } catch(Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  //DBObject query = QueryBuilder.start("_id").in(new String[] {"foo", "bar"}).get();
  //collection.find(query);

  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the tables
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      DeleteResult result =
          collection.withWriteConcern(writeConcern).deleteOne(query);
      if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
        System.err.println("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongoClient != null) {
        return;
      }

      Properties props = getProperties();

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(
          props.getProperty("mongodb.upsert", "false"));

      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://")) {
        System.err.println("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options'. "
            + "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      try {
        MongoClientURI uri = new MongoClientURI(url);

        String uriDb = uri.getDatabase();
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
            && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        } else {
          // If no database is specified in URI, use "ycsb"
          databaseName = "ycsb";

        }

        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();

        mongoClient = new MongoClient(uri);
        database =
            mongoClient.getDatabase(databaseName)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);

        System.out.println("mongo client connection created with " + url);
      } catch (Exception e1) {
        System.err
            .println("Could not initialize MongoDB connection pool for Loader: "
                + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document toInsert = new Document("_id", key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.put(entry.getKey(), entry.getValue().toArray());
      }

      if (batchSize == 1) {
        if (useUpsert) {
          // this is effectively an insert, but using an upsert instead due
          // to current inability of the framework to clean up after itself
          // between test runs.
          collection.replaceOne(new Document("_id", toInsert.get("_id")),
              toInsert, UPDATE_WITH_UPSERT);
        } else {
          collection.insertOne(toInsert);
        }
      } else {
        bulkInserts.add(toInsert);
        if (bulkInserts.size() == batchSize) {
          if (useUpsert) {
            List<UpdateOneModel<Document>> updates = 
                new ArrayList<UpdateOneModel<Document>>(bulkInserts.size());
            for (Document doc : bulkInserts) {
              updates.add(new UpdateOneModel<Document>(
                  new Document("_id", doc.get("_id")),
                  doc, UPDATE_WITH_UPSERT));
            }
            collection.bulkWrite(updates);
          } else {
            collection.insertMany(bulkInserts, INSERT_UNORDERED);
          }
          bulkInserts.clear();
        } else {
          return Status.BATCHED_OK;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);

      FindIterable<Document> findIterable = collection.find(query);
      if (fields != null) {
        Document projection = new Document();
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
      }

      Document queryResult = findIterable.first();


      if (queryResult != null) {
        //fillMap(result, queryResult);
      }
      //ystem.out.println(result.toString());S
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
      }
      Document update = new Document("$set", fieldsToSet);

      UpdateResult result = collection.updateOne(query, update);
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the values from the DBObject.
   * 
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }
  }


  protected void geoFillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      String value = "null";
      if (entry.getValue() != null) {
        value = entry.getValue().toString();
      }
      resultMap.put(entry.getKey(), new StringByteIterator(value));
    }
  }
}

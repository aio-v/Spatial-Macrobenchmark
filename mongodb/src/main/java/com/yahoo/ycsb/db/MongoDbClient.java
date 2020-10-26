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
  
  /** Hardcoded for use case 4, tester. */ // UPDATE THESE TO YOUR DELETED OBJECTS AFTER RUNNING UC 4 ONCE
  // private ObjectId[] toDelete = {
  //     new ObjectId("5e71d429a4031107d8740151"),
  //     new ObjectId("5e71d429a4031107d8740202"),
  //     new ObjectId("5e71d429a4031107d8740164"),
  //     new ObjectId("5e71d429a4031107d8740125"),
  //     new ObjectId("5e71d429a4031107d8740186"),
  //     new ObjectId("5e71d429a4031107d87401ad"),
  //     new ObjectId("5e71d393a4031107d871d90d"),
  //     new ObjectId("5e71d393a4031107d871d8ea"),
  //     new ObjectId("5e71d428a4031107d8740019"),
  //     new ObjectId("5e71d429a4031107d87401dd"),
  //     new ObjectId("5e71d393a4031107d871d935"),
  //     new ObjectId("5e71d393a4031107d871db13"),
  //     new ObjectId("5e71d49ea4031107d875c08c"),
  //     new ObjectId("5e71d487a4031107d87565fe"),
  //     new ObjectId("5e71d3d4a4031107d872caf2"),
  //     new ObjectId("5e71d40da4031107d873990e"),
  //     new ObjectId("5e71d3f8a4031107d8734915"),
  //     new ObjectId("5e71d3b6a4031107d8725e82"),
  //     new ObjectId("5e71d3baa4031107d8726d11"),
  //     new ObjectId("5e71d429a4031107d874022e"),
  //     new ObjectId("5e71d3baa4031107d8726d2f"),
  //     new ObjectId("5e71d3baa4031107d8726d5e"),
  //     new ObjectId("5e71d393a4031107d871db5e"),
  //     new ObjectId("5e71d3e1a4031107d872f864"),
  //     new ObjectId("5e71d434a4031107d8742b4f"),
  //     new ObjectId("5e71d4e1a4031107d876bf1b"),
  //     new ObjectId("5e71d503a4031107d8773ebe"),
  //     new ObjectId("5e71d4e0a4031107d876bc53"),
  //     new ObjectId("5e71d3d3a4031107d872c82d"),
  //     new ObjectId("5e71d4f7a4031107d87713e2"),
  //     new ObjectId("5e71d3d3a4031107d872c804"),
  //     new ObjectId("5e71d55fa4031107d878a205"),
  //     new ObjectId("5e71d4e0a4031107d876bc78"),
  //     new ObjectId("5e71d56ba4031107d878cd7d"),
  //     new ObjectId("5e71d393a4031107d871db35"),
  //     new ObjectId("5e71d56ba4031107d878cd99"),
  //     new ObjectId("5e71d3d0a4031107d872bd67"),
  //     new ObjectId("5e71d3d4a4031107d872ca00"),
  //     new ObjectId("5e71d3d3a4031107d872c9d9"),
  //     new ObjectId("5e71d428a4031107d87400b2"),
  //     new ObjectId("5e71d429a4031107d8740472"),
  //     new ObjectId("5e71d428a4031107d8740081"),
  //     new ObjectId("5e71d393a4031107d871d95c"),
  //     new ObjectId("5e71d348a4031107d870bb32"),
  //     new ObjectId("5e71d428a4031107d874003d"),
  //     new ObjectId("5e71d401a4031107d8736b4b"),
  //     new ObjectId("5e71d468a4031107d874f13d"),
  //     new ObjectId("5e71d4dea4031107d876b3b0"),
  //     new ObjectId("5e71d47ba4031107d87537d2"),
  //     new ObjectId("5e71d434a4031107d8742c86"),
  //     new ObjectId("5e71d363a4031107d87123f4"),
  //     new ObjectId("5e71d4dea4031107d876b392"),
  //     new ObjectId("5e71d56ba4031107d878ce36"),
  //     new ObjectId("5e71d3f8a4031107d8734942"),
  //     new ObjectId("5e71d56ba4031107d878ce5f"),
  //     new ObjectId("5e71d56ba4031107d878ce0d"),
  //     new ObjectId("5e71d366a4031107d8712d09"),
  //     new ObjectId("5e71d56ba4031107d878ceb7"),
  //     new ObjectId("5e71d56ba4031107d878ce90"),
  //     new ObjectId("5e71d428a4031107d8740064"),
  //     new ObjectId("5e71d4faa4031107d8771fef"),
  //     new ObjectId("5e71d473a4031107d8751adc"),
  //     new ObjectId("5e71d508a4031107d8775335"),
  //     new ObjectId("5e71d56ba4031107d878cdeb"),
  //     new ObjectId("5e71d56ba4031107d878cdc4"),
  //     new ObjectId("5e71d4b4a4031107d876148e"),
  //     new ObjectId("5e71d442a4031107d87460bc"),
  //     new ObjectId("5e71d50da4031107d877679c"),
  //     new ObjectId("5e71d368a4031107d87135ec"),
  //     new ObjectId("5e71d50ba4031107d8775e97"),
  //     new ObjectId("5e71d3aba4031107d87235cd"),
  //     new ObjectId("5e71d4b4a4031107d87614b7"),
  //     new ObjectId("5e71d560a4031107d878a5ce"),
  //     new ObjectId("5e71d4c3a4031107d8764b8c"),
  //     new ObjectId("5e71d4c3a4031107d8764ae1"),
  //     new ObjectId("5e71d50da4031107d87767d1"),
  //     new ObjectId("5e71d3b7a4031107d8726319"),
  //     new ObjectId("5e71d55ca4031107d878938e"),
  //     new ObjectId("5e71d488a4031107d8756c96"),
  //     new ObjectId("5e71d487a4031107d87565cd"),
  //     new ObjectId("5e71d34ca4031107d870c9fb"),
  //     new ObjectId("5e71d441a4031107d8745fb3"),
  //     new ObjectId("5e71d4b7a4031107d8762023"),
  //     new ObjectId("5e71d38da4031107d871c48c"),
  //     new ObjectId("5e71d3aba4031107d87235ab"),
  //     new ObjectId("5e71d441a4031107d8745f8f"),
  //     new ObjectId("5e71d41ca4031107d873d339"),
  //     new ObjectId("5e71d41ca4031107d873d312"),
  //     new ObjectId("5e71d429a4031107d874044d"),
  //     new ObjectId("5e71d565a4031107d878b924"),
  //     new ObjectId("5e71d489a4031107d8756daf"),
  //     new ObjectId("5e71d429a4031107d87403da"),
  //     new ObjectId("5e71d510a4031107d877721c"),
  //     new ObjectId("5e71d353a4031107d870e6ae"),
  //     new ObjectId("5e71d38da4031107d871c1ec"),
  //     new ObjectId("5e71d372a4031107d8715d83"),
  //     new ObjectId("5e71d3d4a4031107d872ca7b"),
  //     new ObjectId("5e71d510a4031107d87771f9"),
  //     new ObjectId("5e71d386a4031107d871aa04"),
  //     new ObjectId("5e71d410a4031107d873a216"),
  //     new ObjectId("5e71d354a4031107d870e6d9"),
  //     new ObjectId("5e71d36fa4031107d8715056"),
  //     new ObjectId("5e71d41ca4031107d873d0e3"),
  //     new ObjectId("5e71d41ca4031107d873d0b0"),
  //     new ObjectId("5e71d353a4031107d870e4b5"),
  //     new ObjectId("5e71d442a4031107d8746111"),
  //     new ObjectId("5e71d488a4031107d8756c6c"),
  //     new ObjectId("5e71d3d4a4031107d872ca50"),
  //     new ObjectId("5e71d3faa4031107d873515a"),
  //     new ObjectId("5e71d354a4031107d870e704"),
  //     new ObjectId("5e71d353a4031107d870e48b"),
  //     new ObjectId("5e71d554a4031107d87877d9"),
  //     new ObjectId("5e71d510a4031107d8777242"),
  //     new ObjectId("5e71d353a4031107d870e46b"),
  //     new ObjectId("5e71d3c0a4031107d8728329"),
  //     new ObjectId("5e71d354a4031107d870e8f4"),
  //     new ObjectId("5e71d353a4031107d870e445"),
  //     new ObjectId("5e71d488a4031107d8756b03"),
  //     new ObjectId("5e71d3c0a4031107d872834d"),
  //     new ObjectId("5e71d56ba4031107d878cede"),
  //     new ObjectId("5e71d354a4031107d870e8c5"),
  //     new ObjectId("5e71d4cea4031107d87677fe"),
  //     new ObjectId("5e71d4b9a4031107d87624a9"),
  //     new ObjectId("5e71d36fa4031107d871523d"),
  //     new ObjectId("5e71d441a4031107d8745fd4"),
  //     new ObjectId("5e71d366a4031107d8712ec4"),
  //     new ObjectId("5e71d381a4031107d8719549"),
  //     new ObjectId("5e71d354a4031107d870e8a8"),
  //     new ObjectId("5e71d356a4031107d870f1aa"),
  //     new ObjectId("5e71d3a6a4031107d8722196"),
  //     new ObjectId("5e71d36fa4031107d871520a"),
  //     new ObjectId("5e71d569a4031107d878c88c"),
  //     new ObjectId("5e71d3aca4031107d8723ab1"),
  //     new ObjectId("5e71d510a4031107d87771c6"),
  //     new ObjectId("5e71d56aa4031107d878c953"),
  //     new ObjectId("5e71d36fa4031107d87151e0"),
  //     new ObjectId("5e71d488a4031107d8756a0e"),
  //     new ObjectId("5e71d36fa4031107d871502a"),
  //     new ObjectId("5e71d414a4031107d873b3d4"),
  //     new ObjectId("5e71d3c8a4031107d8729f03"),
  //     new ObjectId("5e71d513a4031107d8777daa"),
  //     new ObjectId("5e71d560a4031107d878a577"),
  //     new ObjectId("5e71d503a4031107d8774022"),
  //     new ObjectId("5e71d353a4031107d870e683"),
  //     new ObjectId("5e71d521a4031107d877b2e4"),
  //     new ObjectId("5e71d352a4031107d870e1ca"),
  //     new ObjectId("5e71d3e7a4031107d8730f14"),
  //     new ObjectId("5e71d354a4031107d870e919"),
  //     new ObjectId("5e71d560a4031107d878a5a6"),
  //     new ObjectId("5e71d489a4031107d8756eeb"),
  //     new ObjectId("5e71d429a4031107d8740419"),
  //     new ObjectId("5e71d3f3a4031107d873375f"),
  //     new ObjectId("5e71d4b4a4031107d87614d1"),
  //     new ObjectId("5e71d3d4a4031107d872ca28"),
  //     new ObjectId("5e71d4cea4031107d87677d1"),
  //     new ObjectId("5e71d3aca4031107d8723a82"),
  //     new ObjectId("5e71d3e7a4031107d8730f39"),
  //     new ObjectId("5e71d352a4031107d870e198"),
  //     new ObjectId("5e71d3d4a4031107d872caa2"),
  //     new ObjectId("5e71d50da4031107d8776775"),
  //     new ObjectId("5e71d353a4031107d870e65e"),
  //     new ObjectId("5e71d552a4031107d8786ebf"),
  //     new ObjectId("5e71d362a4031107d8712023"),
  //     new ObjectId("5e71d368a4031107d87135d2"),
  //     new ObjectId("5e71d50da4031107d8776740"),
  //     new ObjectId("5e71d488a4031107d8756b51"),
  //     new ObjectId("5e71d552a4031107d8786e98"),
  //     new ObjectId("5e71d350a4031107d870d74b"),
  //     new ObjectId("5e71d536a4031107d878021d"),
  //     new ObjectId("5e71d368a4031107d87135ae"),
  //     new ObjectId("5e71d4b7a4031107d8761fff"),
  //     new ObjectId("5e71d399a4031107d871f078"),
  //     new ObjectId("5e71d42aa4031107d874050b"),
  //     new ObjectId("5e71d53aa4031107d87811fc"),
  //     new ObjectId("5e71d503a4031107d8774049"),
  //     new ObjectId("5e71d459a4031107d874b6b2"),
  //     new ObjectId("5e71d508a4031107d877535e"),
  //     new ObjectId("5e71d56ba4031107d878cfc4"),
  //     new ObjectId("5e71d4e1a4031107d876bf40"),
  //     new ObjectId("5e71d56ba4031107d878cf02"),
  //     new ObjectId("5e71d458a4031107d874b3dc"),
  //     new ObjectId("5e71d3d4a4031107d872cacc"),
  //     new ObjectId("5e71d4e3a4031107d876c750"),
  //     new ObjectId("5e71d51ca4031107d8779ecb"),
  //     new ObjectId("5e71d352a4031107d870e2ae"),
  //     new ObjectId("5e71d353a4031107d870e41d"),
  //     new ObjectId("5e71d41ca4031107d873d2c0"),
  //     new ObjectId("5e71d354a4031107d870e941"),
  //     new ObjectId("5e71d352a4031107d870e28c"),
  //     new ObjectId("5e71d41ca4031107d873d2ec"),
  //     new ObjectId("5e71d489a4031107d8756d8f"),
  //     new ObjectId("5e71d503a4031107d8774004"),
  //     new ObjectId("5e71d35ca4031107d8710677"),
  //     new ObjectId("5e71d35da4031107d8710d12"),
  //     new ObjectId("5e71d503a4031107d8774070"),
  //     new ObjectId("5e71d426a4031107d873f7b5"),
  //     new ObjectId("5e71d50fa4031107d8776f93"),
  //     new ObjectId("5e71d3efa4031107d8732aaf"),
  //     new ObjectId("5e71d4b9a4031107d87624d1"),
  //     new ObjectId("5e71d459a4031107d874b560"),
  //     new ObjectId("5e71d503a4031107d8773fdc"),
  //     new ObjectId("5e71d353a4031107d870e3f1"),
  //     new ObjectId("5e71d51aa4031107d877994a"),
  //     new ObjectId("5e71d3a6a4031107d87221ea"),
  //     new ObjectId("5e71d503a4031107d8773faf"),
  //     new ObjectId("5e71d4cea4031107d8767765"),
  //     new ObjectId("5e71d416a4031107d873bc39"),
  //     new ObjectId("5e71d498a4031107d875a99e"),
  //     new ObjectId("5e71d37aa4031107d8717c28"),
  //     new ObjectId("5e71d41ea4031107d873d85d"),
  //     new ObjectId("5e71d4a7a4031107d875e0a1"),
  //     new ObjectId("5e71d46ea4031107d8750827"),
  //     new ObjectId("5e71d373a4031107d8715ed4"),
  //     new ObjectId("5e71d357a4031107d870f602"),
  //     new ObjectId("5e71d503a4031107d8773f84"),
  //     new ObjectId("5e71d438a4031107d8743cef"),
  //     new ObjectId("5e71d354a4031107d870e72d"),
  //     new ObjectId("5e71d368a4031107d8713500"),
  //     new ObjectId("5e71d353a4031107d870e3ca"),
  //     new ObjectId("5e71d41fa4031107d873dbd2"),
  //     new ObjectId("5e71d46fa4031107d8750910"),
  //     new ObjectId("5e71d352a4031107d870e178"),
  //     new ObjectId("5e71d4a7a4031107d875e07c"),
  //     new ObjectId("5e71d48aa4031107d87570e6"),
  //     new ObjectId("5e71d442a4031107d87460e4"),
  //     new ObjectId("5e71d55aa4031107d8788bd7"),
  //     new ObjectId("5e71d41ca4031107d873d089"),
  //     new ObjectId("5e71d41ca4031107d873d064"),
  //     new ObjectId("5e71d3afa4031107d87242df"),
  //     new ObjectId("5e71d53aa4031107d8781122"),
  //     new ObjectId("5e71d37aa4031107d8717c54"),
  //     new ObjectId("5e71d3c1a4031107d87285ea"),
  //     new ObjectId("5e71d53ea4031107d87820e1"),
  //     new ObjectId("5e71d382a4031107d8719809"),
  //     new ObjectId("5e71d447a4031107d8747433"),
  //     new ObjectId("5e71d353a4031107d870e635"),
  //     new ObjectId("5e71d354a4031107d870e969"),
  //     new ObjectId("5e71d503a4031107d8773f3a"),
  //     new ObjectId("5e71d3c0a4031107d8728369"),
  //     new ObjectId("5e71d498a4031107d875a74b"),
  //     new ObjectId("5e71d3c0a4031107d87283b6"),
  //     new ObjectId("5e71d503a4031107d8774099"),
  //     new ObjectId("5e71d388a4031107d871b192"),
  //     new ObjectId("5e71d40da4031107d8739934"),
  //     new ObjectId("5e71d4b8a4031107d8762325"),
  //     new ObjectId("5e71d3b9a4031107d87269e8"),
  //     new ObjectId("5e71d4b8a4031107d8762303"),
  //     new ObjectId("5e71d41fa4031107d873dbf9"),
  //     new ObjectId("5e71d353a4031107d870e605"),
  //     new ObjectId("5e71d552a4031107d8786d2f"),
  //     new ObjectId("5e71d503a4031107d8773f63"),
  //     new ObjectId("5e71d503a4031107d8773f10"),
  //     new ObjectId("5e71d51aa4031107d8779921"),
  //     new ObjectId("5e71d3c0a4031107d8728394"),
  //     new ObjectId("5e71d4b8a4031107d8762354"),
  //     new ObjectId("5e71d56da4031107d878d541"),
  //     new ObjectId("5e71d354a4031107d870e997"),
  //     new ObjectId("5e71d551a4031107d8786cff"),
  //     new ObjectId("5e71d550a4031107d8786848"),
  //     new ObjectId("5e71d3e9a4031107d8731451"),
  //     new ObjectId("5e71d4b8a4031107d876237b"),
  //     new ObjectId("5e71d552a4031107d8786d56"),
  //     new ObjectId("5e71d560a4031107d878a5f9"),
  //     new ObjectId("5e71d4b8a4031107d8762397"),
  //     new ObjectId("5e71d3afa4031107d8724395"),
  //     new ObjectId("5e71d373a4031107d871612c"),
  //     new ObjectId("5e71d551a4031107d8786cd3"),
  //     new ObjectId("5e71d4b8a4031107d87623bf"),
  //     new ObjectId("5e71d429a4031107d87403f6"),
  //     new ObjectId("5e71d560a4031107d878a390"),
  //     new ObjectId("5e71d4e2a4031107d876c47c"),
  //     new ObjectId("5e71d552a4031107d8786d80"),
  //     new ObjectId("5e71d353a4031107d870e3ae"),
  //     new ObjectId("5e71d48aa4031107d87570c4"),
  //     new ObjectId("5e71d373a4031107d8716156"),
  //     new ObjectId("5e71d395a4031107d871e445"),
  //     new ObjectId("5e71d515a4031107d87785c5"),
  //     new ObjectId("5e71d4caa4031107d87665c8"),
  //     new ObjectId("5e71d3fda4031107d8735b2a"),
  //     new ObjectId("5e71d495a4031107d8759d6c"),
  //     new ObjectId("5e71d4b8a4031107d87623e2"),
  //     new ObjectId("5e71d458a4031107d874b1cb"),
  //     new ObjectId("5e71d515a4031107d8778592"),
  //     new ObjectId("5e71d488a4031107d8756b76"),
  //     new ObjectId("5e71d4b9a4031107d8762480"),
  //     new ObjectId("5e71d352a4031107d870df55"),
  //     new ObjectId("5e71d515a4031107d877856f"),
  //     new ObjectId("5e71d4b8a4031107d8762458"),
  //     new ObjectId("5e71d552a4031107d8786da6"),
  //     new ObjectId("5e71d55ca4031107d87893b8"),
  //     new ObjectId("5e71d4b8a4031107d8762431"),
  //     new ObjectId("5e71d552a4031107d8786dcf"),
  //     new ObjectId("5e71d536a4031107d87801aa"),
  //     new ObjectId("5e71d373a4031107d8716170"),
  //     new ObjectId("5e71d552a4031107d8786df8"),
  //     new ObjectId("5e71d4b8a4031107d8762408"),
  //     new ObjectId("5e71d352a4031107d870e12f"),
  //     new ObjectId("5e71d52ba4031107d877d766"),
  //     new ObjectId("5e71d515a4031107d8778542"),
  //     new ObjectId("5e71d366a4031107d8712e97"),
  //     new ObjectId("5e71d39da4031107d8720027"),
  //     new ObjectId("5e71d392a4031107d871d7d2"),
  //     new ObjectId("5e71d515a4031107d877851b"),
  //     new ObjectId("5e71d552a4031107d8786e1f"),
  //     new ObjectId("5e71d3b8a4031107d872646e"),
  //     new ObjectId("5e71d42aa4031107d87404e5"),
  //     new ObjectId("5e71d353a4031107d870e5dc"),
  //     new ObjectId("5e71d352a4031107d870dfa3"),
  //     new ObjectId("5e71d552a4031107d8786e47"),
  //     new ObjectId("5e71d372a4031107d8715d5c"),
  //     new ObjectId("5e71d552a4031107d8786e6d"),
  //     new ObjectId("5e71d42aa4031107d8740530"),
  //     new ObjectId("5e71d386a4031107d871aa7d"),
  //     new ObjectId("5e71d515a4031107d87784ef"),
  //     new ObjectId("5e71d4caa4031107d87665a5"),
  //     new ObjectId("5e71d536a4031107d87801d2"),
  //     new ObjectId("5e71d373a4031107d8716199"),
  //     new ObjectId("5e71d515a4031107d87784c4"),
  //     new ObjectId("5e71d393a4031107d871da74"),
  //     new ObjectId("5e71d396a4031107d871e46a"),
  //     new ObjectId("5e71d515a4031107d87784a4"),
  //     new ObjectId("5e71d365a4031107d87129be"),
  //     new ObjectId("5e71d353a4031107d870e380"),
  //     new ObjectId("5e71d42aa4031107d87404cb"),
  //     new ObjectId("5e71d416a4031107d873baa8"),
  //     new ObjectId("5e71d392a4031107d871d7f8"),
  //     new ObjectId("5e71d393a4031107d871da4c"),
  //     new ObjectId("5e71d393a4031107d871da96"),
  //     new ObjectId("5e71d393a4031107d871dac7"),
  //     new ObjectId("5e71d366a4031107d8712bed"),
  //     new ObjectId("5e71d3afa4031107d87243eb"),
  //     new ObjectId("5e71d356a4031107d870f186"),
  //     new ObjectId("5e71d3d3a4031107d872c982"),
  //     new ObjectId("5e71d3d3a4031107d872c9ae"),
  //     new ObjectId("5e71d374a4031107d87161bf"),
  //     new ObjectId("5e71d55aa4031107d8788c3f"),
  //     new ObjectId("5e71d515a4031107d8778479"),
  //     new ObjectId("5e71d41ea4031107d873d809"),
  //     new ObjectId("5e71d55aa4031107d8788c1b"),
  //     new ObjectId("5e71d480a4031107d8754c92"),
  //     new ObjectId("5e71d352a4031107d870df2d"),
  //     new ObjectId("5e71d498a4031107d875a66d"),
  //     new ObjectId("5e71d393a4031107d871daed"),
  //     new ObjectId("5e71d3faa4031107d8735181"),
  //     new ObjectId("5e71d35ca4031107d8710647"),
  //     new ObjectId("5e71d50da4031107d8776837"),
  //     new ObjectId("5e71d515a4031107d8778451"),
  //     new ObjectId("5e71d47da4031107d8753e8c"),
  //     new ObjectId("5e71d396a4031107d871e514"),
  //     new ObjectId("5e71d393a4031107d871da24"),
  //     new ObjectId("5e71d4b5a4031107d8761865"),
  //     new ObjectId("5e71d36fa4031107d871507f"),
  //     new ObjectId("5e71d55aa4031107d8788bef"),
  //     new ObjectId("5e71d372a4031107d8715dab"),
  //     new ObjectId("5e71d428a4031107d87400d6"),
  //     new ObjectId("5e71d396a4031107d871e493"),
  //     new ObjectId("5e71d496a4031107d8759f3a"),
  //     new ObjectId("5e71d515a4031107d87783ff"),
  //     new ObjectId("5e71d515a4031107d8778423"),
  //     new ObjectId("5e71d388a4031107d871b113"),
  //     new ObjectId("5e71d496a4031107d875a090"),
  //     new ObjectId("5e71d499a4031107d875aa33"),
  //     new ObjectId("5e71d53aa4031107d878133e"),
  //     new ObjectId("5e71d48aa4031107d8757109") 
  // };

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
      if(geoLoad(table2, generator) == Status.ERROR) {
        return Status.ERROR;
      }
      if(geoLoad(table3, generator) == Status.ERROR) {
        return Status.ERROR;
      }
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
//      MongoCursor<Document> cursor = null;

      /* ARTIFICIAL USE CASE */
//      Random random = new Random();
//      ArrayList<DataFilter> predicates = gen.getGeometryPredicatesList();
//      DataFilter school = predicates.get(random.nextInt(2754) + 1);
//      
//      System.out.println(school.getDocid());
//      
//      String nearFieldName = school.getNestedPredicateA().getName();
//
//      HashMap<String, Object> nearFields = new ObjectMapper().readValue(
//          school.getNestedPredicateA().getValueA().toString(), HashMap.class);
//      Document refPoint = new Document(nearFields);
//      
//      Random rand = new Random();
//      double longmax = GeoWorkload.LONG_MAX * -1;
//      double longmin = GeoWorkload.LONG_MIN * -1;
//      double latmax = GeoWorkload.LAT_MAX;
//      double latmin = GeoWorkload.LAT_MIN;
//      
//      double[] coords = {(longmin + (longmax - longmin) * rand.nextDouble()) * -1,
//          latmin + (latmax - latmin) * rand.nextDouble()};
//      JSONObject jobj = new JSONObject().put("coordinates", new JSONArray(coords));
//      jobj.put("type", "Point");
//      HashMap<String, Object> nearFields = new ObjectMapper().readValue(
//          jobj.toString(), HashMap.class);
//      Document refPoint = new Document(nearFields);
//
//      System.out.println(refPoint);
//      
////       Query
//      FindIterable<Document> findIterable = collection.find(Filters.near(
//          "geometry", refPoint, 500.0, 0.0)).batchSize(3000);
//
//       Get all query result's document fields
//      Document projection = new Document();
//      for (String field : gen.getAllGeoFields().get(table)) {
//        projection.put(field, INCLUDE);
//      }
//      findIterable.projection(projection);

      // Add to result
//      Document queryResult = findIterable.first();
//
//      Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
//      if (queryResult != null) {
//        HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
//        geoFillMap(resultMap, queryResult);
//        graffitiResults.add(resultMap);
//      }
//      result.put(school.getName() + refPoint.toString(), graffitiResults);
//      cursor = findIterable.iterator();
//      
//      Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
//      while (cursor.hasNext()) {
//        HashMap<String, ByteIterator> resultMap =
//            new HashMap<String, ByteIterator>();
//
//        Document obj = cursor.next();
//        geoFillMap(resultMap, obj);
//        graffitiResults.add(resultMap);
//      }
//
//      result.put(school.getNestedPredicateA().getValueA().toString(), graffitiResults);
//      result.put(jobj.toString(), graffitiResults);

      /* END ARTIFICIAL USE CASE */
      
      int maxGraffitiCount = Integer.MIN_VALUE;
      String maxGraffitiSchool = "";
      Vector<HashMap<String, ByteIterator>> maxGraffiti = new Vector<>();
      ArrayList<DataFilter> schools = gen.getGeometryPredicatesList();
      // Perform near query on incidents for all school documents
      for(int i = 0; i < schools.size(); i++) {
        DataFilter school = schools.get(i);
        System.out.println("i = " + i + " " + school.getName() + 
            ": " + school.getNestedPredicateA().getValueA().toString());

        String nearFieldName = school.getNestedPredicateA().getName();
        HashMap<String, Object> nearFields = new ObjectMapper().readValue(
            school.getNestedPredicateA().getValueA().toString(), HashMap.class);
        Document refPoint = new Document(nearFields);
        
        // Query
        FindIterable<Document> findIterable = collection.find(Filters.near(
            nearFieldName, refPoint, 500.0, 0.0)).batchSize(2000);
        
        // Get all query result's document fields
        Document projection = new Document();
        for (String field : gen.getAllGeoFields().get(table)) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
        
        // Add to results
        Document d = findIterable.first();
//        ArrayList<Document> graffitiList = findIterable.into(new ArrayList<Document>());
////        cursor = findIterable.iterator();      
  
//        // If there is graffiti, add the results under the school's name
        if (d != null) {
          HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
          geoFillMap(resultMap, d);
          maxGraffiti.add(resultMap);
        }
        
//        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
////        while (cursor.hasNext()) {
//        for(Document d : graffitiList) {
//          HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
//
////          Document obj = cursor.next();
////          geoFillMap(resultMap, obj);
//          geoFillMap(resultMap, d);
//          graffitiResults.add(resultMap);
//        }
//        if(graffitiResults.size() > maxGraffitiCount) {
//          maxGraffitiSchool = school.getName() + school.getNestedPredicateA().getValueA().toString();
//          maxGraffiti = graffitiResults;
//          maxGraffitiCount = maxGraffiti.size();
//
//          System.out.println(maxGraffitiSchool + " graffiti count: " + maxGraffiti.size());
//        } else {
//          System.out.println("PASS...");
//        }
      }
////      
////      if(maxGraffiti == null) {
////        return Status.ERROR;
////      }
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

      ArrayList<DataFilter> neighborhoods = gen.getGeometryPredicatesList();
      // Loop through grid of city
      for(int i = 0; i < neighborhoods.size(); i++) {
        DataFilter cell = neighborhoods.get(i);
        JSONObject intersectFieldValue = cell.getValueA();
        HashMap<String, Object> intersectFields = new ObjectMapper()
            .readValue(intersectFieldValue.toString(), HashMap.class);
        Document refPoint = new Document(intersectFields);
        
        // Query
        FindIterable<Document> findIterable = collection.find(
            Filters.geoWithin("geometry", refPoint)).batchSize(2000);

        // Get all query result's document fields
        Document projection = new Document();
        for (String field : gen.getAllGeoFields().get(table)) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);

        // Add to result
        Document d = findIterable.first();
//        ArrayList<Document> graffitiList = findIterable.into(new ArrayList<Document>());
        
        // If there is graffiti, add the results under the cell's locations
        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
        if (d != null) {
          HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
          geoFillMap(resultMap, d);
          graffitiResults.add(resultMap);
        }
//        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
//        for(Document d : graffitiList) {
//          HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
//          geoFillMap(resultMap, d);
//          graffitiResults.add(resultMap);
//        }
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
            Filters.geoIntersects(fieldName, refPoint)).batchSize(2000);
        
        // Project
        Document projection = new Document();
        for (String field : gen.getAllGeoFields().get(table1)) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
        
        // Add to result
        ArrayList<Document> buildingList = findIterable.into(new ArrayList<Document>());
        
        // Key of the grid cell = intersectFieldValue
        // If no buildings in the cell, put value to 0
        if(buildingList.size() == 0) {
          densities.put(intersectFieldValue, 0.0);
          continue;
        }
        // If there are buildings, sum their areas
        double density = 0;
        for(Document d : buildingList) {
          JSONObject jobj = new JSONObject(d.toJson());
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
            Filters.geoIntersects("geometry", refPoint)).batchSize(2000);
        
        // Project
        Document projection = new Document();
        for (String field : gen.getAllGeoFields().get(table2)) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
        
        // Add to result
        Document d = findIterable.first();
//      ArrayList<Document> graffitiList = findIterable.into(new ArrayList<Document>());
      
        // If there is graffiti, add the result under the cell's locations
        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
        if (d != null) {
          HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
          geoFillMap(resultMap, d);
          graffitiResults.add(resultMap);
        }
        //If there is no graffiti, add an empty vector
//        if(graffitiList.size() == 0) {
//          result.put(intersectFieldValue, new Vector<HashMap<String, ByteIterator>>());
//          continue;
//        }
//        
//        // If there is graffiti, add the results under the cell's locations
//        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
//        for(Document d : graffitiList) {
//          HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
//          geoFillMap(resultMap, d);
//          graffitiResults.add(resultMap);
//        }
        result.put(intersectFieldValue.toString(), graffitiResults);
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
    HashMap<String, Vector<HashMap<String, ByteIterator>>> toDelete = new HashMap<>();
    try {
      Status queryStatus = null;
      // Based on the operation, clean all the graffiti resulting from that search
      switch(operation) {
      case "geo_case_graffiti_by_schools":
        queryStatus = geoUseCase1(table, toDelete, gen);
        break;
      default:
        return Status.ERROR;
      }
      
      if(queryStatus == Status.ERROR) {
        return Status.ERROR;
      }
      
      MongoCollection<Document> collection = database.getCollection(table);
      
      int counter = 0;
      // deletes query result
      for(String key : toDelete.keySet()) {
        System.out.println("Graffiti in " + key + ": " + toDelete.get(key).size());
        for(HashMap<String, ByteIterator> doc : toDelete.get(key)) {
          System.out.println(doc.get("_id").toString());
          //delete
          BasicDBObject delete = new BasicDBObject();
          delete.put("_id", new ObjectId(doc.get("_id").toString()));
          counter += collection.deleteOne(delete).getDeletedCount();
        }
      }
      System.out.println("\tDeleted: " + counter);
      
      // deletes hardcoded values
//      for(ObjectId id : toDelete) { 
//        BasicDBObject delete = new BasicDBObject();
//        delete.put("_id", id);
//        counter += collection.deleteOne(delete).getDeletedCount();
//      }
//      System.out.println("\tDeleted: " + counter);

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

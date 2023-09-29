/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.mongodb;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.testcontainers.containers.MongoDBContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public abstract class AbstractMongoTest extends SimpleTestInClusterSupport {
    /**
     * Version of MongoDB Container that will be used in the tests.
     */
    public static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "7.0.0");

    static MongoClient mongo;
    static BsonTimestamp startAtOperationTime;

    private static final Map<String, String> TEST_NAME_TO_DEFAULT_DB_NAME = new ConcurrentHashMap<>();
    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static MongoDBContainer mongoContainer = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void setUp() {
        assumeDockerEnabled();
        mongoContainer.start();
        mongo = MongoClients.create(mongoContainer.getConnectionString());

        // workaround to obtain a timestamp before starting the test
        // If you pass a timestamp which is not in the oplog, mongodb throws exception
        MongoCollection<Document> collection = mongo.getDatabase("tech").getCollection("START_AT_OPERATION");
        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        collection.insertOne(new Document("test", 1));
        startAtOperationTime = cursor.next().getClusterTime();
        cursor.close();

        Config config = new Config();
        config.addMapConfig(new MapConfig("*").setEventJournalConfig(new EventJournalConfig().setEnabled(true)));
        config.addDataConnectionConfig(new DataConnectionConfig("mongoDB")
                .setType("Mongo")
                .setName("mongoDB")
                .setShared(true)
                .setProperty("connectionString", mongoContainer.getConnectionString())
        );
        config.getJetConfig().setEnabled(true);
        initialize(2, config);
    }

    @Before
    public void createDefaultCollection() {
        mongo.getDatabase(defaultDatabase()).createCollection(testName.getMethodName());
    }

    @After
    public void clear() {
        if (mongoContainer != null) {
            try (MongoClient mongoClient = MongoClients.create(mongoContainer.getConnectionString())) {
                for (String databaseName : mongoClient.listDatabaseNames()) {
                    if (databaseName.startsWith("test")) {
                        MongoDatabase database = mongoClient.getDatabase(databaseName);
                        database.drop();
                    }
                }
                List<String> allowedDatabasesLeft = asList("admin", "local", "config", "tech");
                assertTrueEventually(() -> {
                    ArrayList<String> databasesLeft = mongoClient.listDatabaseNames().into(new ArrayList<>());
                    assertEquals(allowedDatabasesLeft.size(), databasesLeft.size());
                    assertContainsAll(databasesLeft, allowedDatabasesLeft);
                });
            }
        }
    }

    @AfterClass
    public static void tearDown() {
        closeResource(mongo);
        closeResource(mongoContainer);
    }

    MongoCollection<Document> collection() {
        return collection(defaultDatabase(), testName.getMethodName());
    }

    protected String defaultDatabase() {
        return TEST_NAME_TO_DEFAULT_DB_NAME.computeIfAbsent(testName.getMethodName(),
                name -> "testDefaultDatabase" + COUNTER.incrementAndGet());
    }

    MongoCollection<Document> collection(String collectionName) {
        return collection(defaultDatabase(), collectionName);
    }

    MongoCollection<Document> collection(String databaseName, String collectionName) {
        return mongo.getDatabase(databaseName).getCollection(collectionName).withCodecRegistry(defaultCodecRegistry());
    }

    static MongoClient mongoClient(String connectionString) {
        return mongoClient(connectionString, 30);
    }

    static MongoClient mongoClient(String connectionString, int timeoutSeconds) {
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(timeoutSeconds, SECONDS))
                .build();

        return MongoClients.create(settings);
    }

}

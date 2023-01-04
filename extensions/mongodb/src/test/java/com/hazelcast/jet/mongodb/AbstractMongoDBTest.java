/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AbstractMongoDBTest extends SimpleTestInClusterSupport {

    static final String SOURCE_NAME = "source";
    static final String SINK_NAME = "sink";
    static final String DB_NAME = "db";

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("mongo:6.0.3");
    MongoClient mongo;
    HazelcastInstance hz;
    BsonTimestamp startAtOperationTime;

    @Rule
    public TestName testName = new TestName();
    @Rule
    public MongoDBContainer mongoContainer = new MongoDBContainer(DOCKER_IMAGE_NAME);

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void setUp() {
        mongoContainer.start();
        mongo = MongoClients.create(mongoContainer.getConnectionString());

        // workaround to obtain a timestamp before starting the test
        // If you pass a timestamp which is not in the oplog, mongodb throws exception
        MongoCollection<Document> collection = mongo.getDatabase("tech").getCollection("START_AT_OPERATION");
        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        collection.insertOne(new Document("key", "val"));
        startAtOperationTime = cursor.next().getClusterTime();
        cursor.close();
    }

    @After
    public void clear() {
        try (MongoClient mongoClient = MongoClients.create(mongoContainer.getConnectionString())) {
            mongoClient.getDatabase(DB_NAME).drop();
        }
    }

    MongoCollection<Document> collection() {
        return collection(DB_NAME, testName.getMethodName());
    }

    MongoCollection<Document> collection(String collectionName) {
        return collection(DB_NAME, collectionName);
    }

    MongoCollection<Document> collection(String databaseName, String collectionName) {
        return mongo.getDatabase(databaseName).getCollection(collectionName);
    }

    static MongoClient mongoClient(String connectionString) {
        return mongoClient(connectionString, 30);
    }

    static MongoClient mongoClient(String connectionString, int timeoutSeconds) {
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(b -> {
                    b.serverSelectionTimeout(timeoutSeconds, SECONDS);
                })
                .build();

        return MongoClients.create(settings);
    }

    @After
    public void tearDown() {
        mongo.close();
    }

}

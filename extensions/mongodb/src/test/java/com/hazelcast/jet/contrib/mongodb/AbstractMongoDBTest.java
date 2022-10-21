/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.mongodb;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

public abstract class AbstractMongoDBTest extends JetTestSupport {

    static final String SOURCE_NAME = "source";
    static final String SINK_NAME = "sink";
    static final String DB_NAME = "db";
    static final String COL_NAME = "col";

    @Rule
    public MongoDBContainer mongoContainer = new MongoDBContainer().withReplicaSetName("rs0");

    MongoClient mongo;
    HazelcastInstance hz;
    BsonTimestamp startAtOperationTime;


    @Before
    public void setUp() {
        mongoContainer.initializeReplicaSet();
        mongo = mongoContainer.newMongoClient();

        // workaround to obtain a timestamp before starting the test
        // If you pass a timestamp which is not in the oplog, mongodb throws exception
        MongoCollection<Document> collection = collection("START_AT_OPERATION");
        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        collection.insertOne(new Document("key", "val"));
        startAtOperationTime = cursor.next().getClusterTime();
        cursor.close();

        hz = createHazelcastInstance();
    }

    MongoCollection<Document> collection() {
        return collection(DB_NAME, COL_NAME);
    }

    MongoCollection<Document> collection(String collectionName) {
        return collection(DB_NAME, collectionName);
    }

    MongoCollection<Document> collection(String databaseName, String collectionName) {
        return mongo.getDatabase(databaseName).getCollection(collectionName);
    }

    @After
    public void tearDown() {
        mongo.close();
    }

}

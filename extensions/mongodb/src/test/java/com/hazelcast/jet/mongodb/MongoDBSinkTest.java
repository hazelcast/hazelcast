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

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Test;

import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.mongodb.MongoDBSourceTest.mongoClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoDBSinkTest extends AbstractMongoDBTest {

    @Test
    public void test() {
        IList<Integer> list = hz.getList("list");
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }

        String connectionString = mongoContainer.getConnectionString();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
         .map(i -> new Document("key", i))
         .writeTo(MongoDBSinks.mongodb(SINK_NAME, connectionString, DB_NAME, COL_NAME));

        hz.getJet().newJob(p).join();

        MongoCollection<Document> collection = collection();
        assertEquals(100, collection.countDocuments());
    }

    @Test
    public void test_whenServerNotAvailable() {
        IList<Integer> list = hz.getList("list");
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }

        Sink<Document> sink = MongoDBSinks
                .<Document>builder(SINK_NAME, () -> mongoClient("non-existing-server", 0))
                .databaseFn(client -> client.getDatabase(DB_NAME))
                .collectionFn(db -> db.getCollection(COL_NAME))
                .destroyFn(MongoClient::close)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
         .map(i -> new Document("key", i))
         .writeTo(sink);

        try {
            hz.getJet().newJob(p).join();
            fail();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof JetException);
        }
    }


}

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

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoDBSinkTest extends AbstractMongoDBTest {

    @Test
    public void test() {
        MongoCollection<Document> collection = collection(defaultDatabase(), testName.getMethodName());
        IList<Integer> list = instance().getList("list");
        final int count = 40_000;
        for (int i = 0; i < count / 2; i++) {
            list.add(i);
        }
        List<Document> docsToUpdate = new ArrayList<>();
        for (int i = count/2; i < count; i++) {
            docsToUpdate.add(new Document("key", i).append("val", i + 100_000).append("some", "text lorem ipsum etc"));
        }
        Collection<String> ids = collection.insertMany(docsToUpdate).getInsertedIds().values().stream()
                                                 .map(id -> id.asObjectId().getValue().toHexString())
                                                 .collect(Collectors.toList());

        String connectionString = mongoContainer.getConnectionString();

        // used to distinguish Documents read from second source, where IDs are count/2 and higher
        int keyDiscriminator = (count/2) + 100;

        Pipeline pipeline = Pipeline.create();
        BatchStage<Document> toAddSource = pipeline.readFrom(Sources.list(list))
                .map(i -> new Document("key", i).append("val", i + 100_000).append("some", "text lorem ipsum etc"))
                .setLocalParallelism(2);

        BatchStage<Document> alreadyExistingSource = pipeline.readFrom(TestSources.items(ids))
                                                             .mapStateful(() -> new AtomicLong(count / 2 + 1),
                                                                     (counter, i) -> new Document("key", keyDiscriminator)
                                                                     .append("_id", new ObjectId(i))
                                                                     .append("val", counter.incrementAndGet())
                                                                     .append("some", "text lorem ipsum etc"))
                                                             .setLocalParallelism(2);

        toAddSource.merge(alreadyExistingSource)
                .rebalance(doc -> doc.get("val")).setLocalParallelism(8)
                .writeTo(MongoDBSinks.mongodb(SINK_NAME, connectionString, defaultDatabase(), testName.getMethodName()))
                .setLocalParallelism(4);

        instance().getJet().newJob(pipeline).join();

        assertEquals(count, collection.countDocuments());
        assertEquals(count/2, collection.countDocuments(Filters.eq("key", keyDiscriminator)));
        assertEquals(count/2, collection.countDocuments(Filters.ne("key", keyDiscriminator)));
    }

    @Test
    public void test_whenServerNotAvailable() {
        IList<Integer> list = instance().getList("list");
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }

        String defaultDatabase = defaultDatabase();
        String collectionName = testName.getMethodName();
        Sink<Document> sink = MongoDBSinks
                .builder(SINK_NAME, Document.class, () -> mongoClient("non-existing-server", 0))
                .databaseName(defaultDatabase)
                .collectionName(collectionName)
                .documentIdentityFn((doc) -> doc.get("_id"))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
         .map(i -> new Document("key", i))
         .writeTo(sink);

        try {
            instance().getJet().newJob(p).join();
            fail();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof JetException);
        }
    }


}

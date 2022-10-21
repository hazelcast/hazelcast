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

import com.hazelcast.collection.IList;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MongoDBSourceTest extends AbstractMongoDBTest {

    @Test
    public void testStream_whenServerDown() {
        HazelcastInstance serverToShutdown = createHazelcastInstance();

        int itemCount = 40_000;

        Sink<Integer> setSink = SinkBuilder
                .sinkBuilder("set", c -> c.hazelcastInstance().getSet("set"))
                .<Integer>receiveFn(Set::add)
                .build();


        Pipeline p = Pipeline.create();
        p.readFrom(streamSource(30))
         .withNativeTimestamps(0)
         .map(doc -> doc.getInteger("key"))
         .writeTo(setSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                 .setSnapshotIntervalMillis(2000);
        Job job = hz.getJet().newJob(p, jobConfig);
        ISet<Integer> set = hz.getSet("set");

        spawn(() -> {
            for (int i = 0; i < itemCount; i++) {
                collection().insertOne(new Document("key", i));
            }
        });

        sleepSeconds(5);

        serverToShutdown.shutdown();

        assertTrueEventually(() -> {
            assertEquals(itemCount, set.size());
        });

        job.cancel();

    }

    @Test
    public void testBatch() {

        IList<Document> list = hz.getList("list");

        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(new Document("key", i).append("val", i));
        }
        collection().insertMany(documents);


        String connectionString = mongoContainer.connectionString();

        Pipeline p = Pipeline.create();
        p.readFrom(MongoDBSources.batch(SOURCE_NAME, connectionString, DB_NAME, COL_NAME,
                new Document("val", new Document("$gte", 10)),
                new Document("val", 1).append("_id", 0)))
         .writeTo(Sinks.list(list));

        hz.getJet().newJob(p).join();

        assertEquals(90, list.size());
        Document actual = list.get(0);
        assertNull(actual.get("key"));
        assertNull(actual.get("_id"));
        assertNotNull(actual.get("val"));
    }

    @Test
    public void testStream() {
        IList<Document> list = hz.getList("list");

        StreamSource<? extends Document> streamSource =
                streamSource(
                        new Document("fullDocument.val", new Document("$gte", 10)).append("operationType", "insert"),
                        new Document("fullDocument.val", 1).append("_id", 1),
                        30
                );

        Pipeline p = Pipeline.create();
        p.readFrom(streamSource)
         .withNativeTimestamps(0)
         .writeTo(Sinks.list(list));

        Job job = hz.getJet().newJob(p);


        collection().insertOne(new Document("val", 1));
        collection().insertOne(new Document("val", 10).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(1, list.size());
            Document document = list.get(0);
            assertEquals(10, document.get("val"));
            assertNull(document.get("foo"));
        });

        collection().insertOne(new Document("val", 2));
        collection().insertOne(new Document("val", 20).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(2, list.size());
            Document document = list.get(1);
            assertEquals(20, document.get("val"));
            assertNull(document.get("foo"));
        });

        job.cancel();

    }

    @Test
    public void testStream_whenWatchDatabase() {
        IList<Document> list = hz.getList("list");

        String connectionString = mongoContainer.connectionString();
        long value = startAtOperationTime.getValue();

        StreamSource<? extends Document> source = MongoDBSourceBuilder
                .streamDatabase(SOURCE_NAME, () -> MongoClients.create(connectionString))
                .databaseFn(client -> client.getDatabase(DB_NAME))
                .destroyFn(MongoClient::close)
                .searchFn(db -> {
                    List<Bson> aggregates = new ArrayList<>();
                    aggregates.add(Aggregates.match(new Document("fullDocument.val", new Document("$gte", 10))
                            .append("operationType", "insert")));

                    aggregates.add(Aggregates.project(new Document("fullDocument.val", 1).append("_id", 1)));
                    return db.watch(aggregates);
                })
                .mapFn(ChangeStreamDocument::getFullDocument)
                .startAtOperationTimeFn(client -> new BsonTimestamp(value))
                .build();


        Pipeline p = Pipeline.create();
        p.readFrom(source)
         .withNativeTimestamps(0)
         .writeTo(Sinks.list(list));

        Job job = hz.getJet().newJob(p);

        MongoCollection<Document> col1 = collection("col1");
        MongoCollection<Document> col2 = collection("col2");

        col1.insertOne(new Document("val", 1));
        col1.insertOne(new Document("val", 10).append("foo", "bar"));

        col2.insertOne(new Document("val", 2));
        col2.insertOne(new Document("val", 11).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(2, list.size());
            list.forEach(document -> assertNull(document.get("foo")));

            assertEquals(10, list.get(0).get("val"));
            assertEquals(11, list.get(1).get("val"));

        });

        col1.insertOne(new Document("val", 3));
        col1.insertOne(new Document("val", 12).append("foo", "bar"));

        col2.insertOne(new Document("val", 4));
        col2.insertOne(new Document("val", 13).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(4, list.size());
            list.forEach(document -> assertNull(document.get("foo")));

            assertEquals(12, list.get(2).get("val"));
            assertEquals(13, list.get(3).get("val"));
        });

        job.cancel();

    }

    @Test
    public void testStream_whenWatchAll() {
        IList<Document> list = hz.getList("list");

        String connectionString = mongoContainer.connectionString();
        long value = startAtOperationTime.getValue();

        StreamSource<? extends Document> source = MongoDBSourceBuilder
                .streamAll(SOURCE_NAME, () -> MongoClients.create(connectionString))
                .destroyFn(MongoClient::close)
                .searchFn(client -> {
                    List<Bson> aggregates = new ArrayList<>();
                    aggregates.add(Aggregates.match(new Document("fullDocument.val", new Document("$gt", 10))
                            .append("operationType", "insert")));

                    aggregates.add(Aggregates.project(new Document("fullDocument.val", 1).append("_id", 1)));
                    return client.watch(aggregates);
                })
                .mapFn(ChangeStreamDocument::getFullDocument)
                .startAtOperationTimeFn(client -> new BsonTimestamp(value))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(source)
         .withNativeTimestamps(0)
         .writeTo(Sinks.list(list));

        Job job = hz.getJet().newJob(p);

        MongoCollection<Document> col1 = collection("db1", "col1");
        MongoCollection<Document> col2 = collection("db1", "col2");
        MongoCollection<Document> col3 = collection("db2", "col3");

        col1.insertOne(new Document("val", 1));
        col1.insertOne(new Document("val", 11).append("foo", "bar"));
        col2.insertOne(new Document("val", 2));
        col2.insertOne(new Document("val", 12).append("foo", "bar"));
        col3.insertOne(new Document("val", 3));
        col3.insertOne(new Document("val", 13).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(3, list.size());
            list.forEach(document -> assertNull(document.get("foo")));

            assertEquals(11, list.get(0).get("val"));
            assertEquals(12, list.get(1).get("val"));
            assertEquals(13, list.get(2).get("val"));
        });

        col1.insertOne(new Document("val", 4));
        col1.insertOne(new Document("val", 14).append("foo", "bar"));
        col2.insertOne(new Document("val", 5));
        col2.insertOne(new Document("val", 15).append("foo", "bar"));
        col2.insertOne(new Document("val", 6));
        col2.insertOne(new Document("val", 16).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(6, list.size());
            list.forEach(document -> assertNull(document.get("foo")));

            assertEquals(14, list.get(3).get("val"));
            assertEquals(15, list.get(4).get("val"));
            assertEquals(16, list.get(5).get("val"));
        });

        job.cancel();

    }

    private StreamSource<? extends Document> streamSource(int connectionTimeoutSeconds) {
        return streamSource(null, null, connectionTimeoutSeconds);
    }

    private StreamSource<? extends Document> streamSource(
            Document filter,
            Document projection,
            int connectionTimeoutSeconds
    ) {
        String connectionString = mongoContainer.connectionString();
        long value = startAtOperationTime.getValue();
        return MongoDBSourceBuilder
                .stream(SOURCE_NAME, () -> mongoClient(connectionString, connectionTimeoutSeconds))
                .databaseFn(client -> client.getDatabase(DB_NAME))
                .collectionFn(db -> db.getCollection(COL_NAME))
                .destroyFn(MongoClient::close)
                .searchFn(col -> {
                    List<Bson> aggregates = new ArrayList<>();
                    if (filter != null) {
                        aggregates.add(Aggregates.match(filter));
                    }
                    if (projection != null) {
                        aggregates.add(Aggregates.project(projection));
                    }
                    ChangeStreamIterable<? extends Document> watch;
                    if (aggregates.isEmpty()) {
                        watch = col.watch();
                    } else {
                        watch = col.watch(aggregates);
                    }
                    return watch;
                })
                .mapFn(ChangeStreamDocument::getFullDocument)
                .startAtOperationTimeFn(client -> new BsonTimestamp(value))
                .build();
    }

    static MongoClient mongoClient(String connectionString, int connectionTimeoutSeconds) {
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(b -> {
                    b.serverSelectionTimeout(connectionTimeoutSeconds, SECONDS);
                })
                .build();

        return MongoClients.create(settings);
    }


}

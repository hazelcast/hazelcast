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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.jet.mongodb.MongoDBSinks.builder;
import static com.hazelcast.jet.mongodb.MongoDBSinks.mongodb;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sources.mapJournal;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.ne;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class MongoDBSinkTest extends AbstractMongoDBTest {

    private static final long COUNT = 2000;
    private static final long HALF = COUNT / 2;

    @Parameter(0)
    public ProcessingGuarantee processingGuarantee;

    @Parameters(name = "processing guarantee: {0}")
    public static Object[] guarantees() {
        return ProcessingGuarantee.values();
    }

    @Test
    public void test_withBatchSource() {
        IList<Long> list = instance().getList(testName.getMethodName());
        MongoCollection<Document> collection = collection(defaultDatabase(), testName.getMethodName());
        for (long i = 0; i < HALF; i++) {
            list.add(i);
        }
        List<Document> docsToUpdate = new ArrayList<>();
        for (long i = HALF; i < COUNT; i++) {
            docsToUpdate.add(new Document("key", i).append("val", i + 100_000).append("some", "text lorem ipsum etc"));
        }
        Collection<String> ids = collection.insertMany(docsToUpdate).getInsertedIds().values().stream()
                                                 .map(id -> id.asObjectId().getValue().toHexString())
                                                 .collect(toList());

        String connectionString = mongoContainer.getConnectionString();

        // used to distinguish Documents read from second source, where IDs are count/2 and higher
        long originStreamDiscriminator = HALF + 100;

        Pipeline pipeline = Pipeline.create();
        BatchStage<Document> toAddSource = pipeline.readFrom(Sources.list(list))
                                                   .map(i -> new Document("key", i)
                                                           .append("val", i)
                                                           .append("type", "new")
                                                           .append("some", "text lorem ipsum etc")
                                                   )
                                                   .setLocalParallelism(2);

        BatchStage<Document> alreadyExistingSource = pipeline.readFrom(TestSources.items(ids))
                                                             .mapStateful(() -> new AtomicLong(HALF + 1),
                                                                     (counter, i) -> new Document("key",
                                                                             counter.incrementAndGet())
                                                                             .append("_id", new ObjectId(i))
                                                                             .append("val", originStreamDiscriminator)
                                                                             .append("type", "existing")
                                                                             .append("some", "text lorem ipsum etc"))
                                                             .setLocalParallelism(2);

        toAddSource.merge(alreadyExistingSource).setLocalParallelism(8)
                   .rebalance(doc -> doc.get("key")).setLocalParallelism(8)
                   .writeTo(mongodb(SINK_NAME, connectionString, defaultDatabase(), testName.getMethodName()))
                   .setLocalParallelism(1);

        JobConfig config = new JobConfig().setProcessingGuarantee(processingGuarantee).setSnapshotIntervalMillis(500);
        instance().getJet().newJob(pipeline, config).join();

        assertEquals(COUNT, collection.countDocuments());
        assertEquals(HALF, collection.countDocuments(eq("val", originStreamDiscriminator)));
        assertEquals(HALF, collection.countDocuments(ne("val", originStreamDiscriminator)));
    }

    @Test
    public void test_withStreamAsInput() {
        MongoCollection<Document> collection = collection(defaultDatabase(), testName.getMethodName());
        IMap<Long, Long> mapToInsert = instance().getMap("toInsert");
        for (long i = 0; i < HALF; i++) {
            mapToInsert.put(i, i);
        }
        List<Document> docsToUpdate = new ArrayList<>();
        for (long i = HALF; i < COUNT; i++) {
            docsToUpdate.add(new Document("key", i).append("val", i + 100_000).append("some", "text lorem ipsum etc"));
        }
        IMap<String, String> mapToUpdate = instance().getMap("toUpdate");
        collection.insertMany(docsToUpdate).getInsertedIds().values().stream()
                                                 .map(id -> id.asObjectId().getValue().toHexString())
                                                 .forEach(id -> mapToUpdate.put(id, id));


        String connectionString = mongoContainer.getConnectionString();

        // used to distinguish Documents read from second source, where IDs are COUNT/2 and higher
        long streamOriginDiscriminator = HALF + 100;

        Pipeline pipeline = Pipeline.create();
        StreamStage<Doc> toAddSource = pipeline
                .readFrom(mapJournal(mapToInsert, START_FROM_OLDEST,
                        EventJournalMapEvent::getNewValue, e -> e.getType() == ADDED))
                .withIngestionTimestamps()
                .map(i -> new Doc(null, i, i, "text lorem ipsum etc")
                )
                .setLocalParallelism(2);

        StreamStage<Doc> alreadyExistingSource = pipeline
                .readFrom(mapJournal(mapToUpdate, START_FROM_OLDEST,
                        EventJournalMapEvent::getNewValue, e -> e.getType() == ADDED))
                .withIngestionTimestamps()
                .mapStateful(() -> new AtomicLong(HALF + 1),
                        (counter, objectIdHex) -> new Doc(new ObjectId(objectIdHex),
                                counter.incrementAndGet(),
                                streamOriginDiscriminator,
                                "text lorem ipsum etc"
                        ))
                .setLocalParallelism(2);

        final String defaultDatabase = defaultDatabase();

        Sink<Doc> sink = builder(SINK_NAME, Doc.class, () -> createClient(connectionString))
                .identifyDocumentBy("key", o -> o.key)
                .into(i -> defaultDatabase, i -> "col_" + (i.key % 10))
                .withCustomReplaceOptions(opt -> opt.upsert(false))
                .build();
        toAddSource
                .merge(alreadyExistingSource)
                   .setLocalParallelism(8)
                   .rebalance(doc -> doc.key).setLocalParallelism(8)
                   .writeTo(sink)
                   .setLocalParallelism(1);

        JobConfig config = new JobConfig().setProcessingGuarantee(processingGuarantee).setSnapshotIntervalMillis(200);
        instance().getJet().newJob(pipeline, config);

        MongoClient client = MongoClients.create(connectionString);
        MongoDatabase db = client.getDatabase(defaultDatabase);
        assertTrueEventually(() -> assertEquals(COUNT, countInAll(db, gte("val", 0))));
        assertEquals(HALF, countInAll(db, eq("val", streamOriginDiscriminator)));
        assertEquals(HALF, countInAll(db, ne("val", streamOriginDiscriminator)));
    }

    private static MongoClient createClient(String connectionString) {
        MongoClientSettings mcs = MongoClientSettings.builder()
                                                     .retryReads(true)
                                                     .retryWrites(true)
                                                     .applyConnectionString(new ConnectionString(connectionString))
                                                     .build();
        return MongoClients.create(mcs);
    }

    private static long countInAll(MongoDatabase database, Bson filter) {
        long count = 0;
        for (String name : database.listCollectionNames()) {
            if (name.startsWith("col_")) {
                count += database.getCollection(name).countDocuments(filter);
            }
        }
        return count;
    }

    @SuppressWarnings("unused") // for serialization
    public static final class Doc implements Serializable {
        @BsonProperty("_id")
        private ObjectId id;
        private Long key;
        private Long val;
        private String some;

        Doc() {
        }

        public Doc(ObjectId id, Long key, Long val, String some) {
            this.id = id;
            this.key = key;
            this.val = val;
            this.some = some;
        }

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
            this.id = id;
        }

        public Long getKey() {
            return key;
        }

        public void setKey(Long key) {
            this.key = key;
        }

        public Long getVal() {
            return val;
        }

        public void setVal(Long val) {
            this.val = val;
        }

        public String getSome() {
            return some;
        }

        public void setSome(String some) {
            this.some = some;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Doc doc = (Doc) o;
            return Objects.equals(id, doc.id) && Objects.equals(key, doc.key)
                    && Objects.equals(val, doc.val) && Objects.equals(some, doc.some);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, key, val, some);
        }
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
                .into(defaultDatabase, collectionName)
                .identifyDocumentBy("_id", (doc) -> doc.get("_id"))
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

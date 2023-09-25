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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.types.ObjectId;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.jet.mongodb.ResourceChecks.NEVER;
import static com.hazelcast.jet.mongodb.WriteMode.INSERT_ONLY;
import static com.hazelcast.jet.mongodb.MongoSinks.builder;
import static com.hazelcast.jet.mongodb.MongoSinks.mongodb;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sources.mapJournal;
import static com.hazelcast.jet.pipeline.test.TestSources.items;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class MongoSinkTest extends AbstractMongoTest {

    private static final long COUNT = 4;
    private static final long HALF = COUNT / 2;

    @Parameter
    public ProcessingGuarantee processingGuarantee;

    @Parameters(name = "processing guarantee: {0}")
    public static Object[] guarantees() {
        return ProcessingGuarantee.values();
    }

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();
    }

    @Test
    public void test_withBatchSource() {
        MongoCollection<Document> collection = collection(defaultDatabase(), testName.getMethodName());
        List<Document> docsToUpdate = new ArrayList<>();
        docsToUpdate.add(new Document("key", 1).append("val", 11).append("type", "existing"));
        docsToUpdate.add(new Document("key", 2).append("val", 11).append("type", "existing"));
        collection.insertMany(docsToUpdate);

        String connectionString = mongoContainer.getConnectionString();

        Pipeline pipeline = Pipeline.create();
        BatchStage<Document> toAddSource = pipeline.readFrom(items(
                                                           new Document("key", 3).append("type", "new"),
                                                           new Document("key", 4).append("type", "new")
                                                   ));

        BatchStage<Document> alreadyExistingSource = pipeline.readFrom(items(docsToUpdate));

        toAddSource.merge(alreadyExistingSource).setLocalParallelism(4)
                   .rebalance(doc -> doc.get("key")).setLocalParallelism(4)
                   .writeTo(mongodb(connectionString, defaultDatabase(), testName.getMethodName()))
                   .setLocalParallelism(2);

        JobConfig config = new JobConfig().setProcessingGuarantee(processingGuarantee).setSnapshotIntervalMillis(500);
        instance().getJet().newJob(pipeline, config).join();

        assertTrueEventually(() -> assertEquals(COUNT, collection.countDocuments()));
        assertEquals(HALF, collection.countDocuments(eq("type", "existing")));
        assertEquals(HALF, collection.countDocuments(eq("type", "new")));
    }

    @Test
    public void test_withBatchSource_andDataConnectionRef() {
        MongoCollection<Document> collection = collection(defaultDatabase(), testName.getMethodName());
        List<Document> docsToUpdate = new ArrayList<>();
        docsToUpdate.add(new Document("key", 1).append("val", 11).append("type", "existing"));
        docsToUpdate.add(new Document("key", 2).append("val", 11).append("type", "existing"));
        collection.insertMany(docsToUpdate);

        Pipeline pipeline = Pipeline.create();
        BatchStage<Document> toAddSource = pipeline.readFrom(items(
                                                           new Document("key", 3).append("type", "new"),
                                                           new Document("key", 4).append("type", "new")
                                                   ));

        BatchStage<Document> alreadyExistingSource = pipeline.readFrom(items(docsToUpdate));

        toAddSource.merge(alreadyExistingSource).setLocalParallelism(4)
                   .rebalance(doc -> doc.get("key")).setLocalParallelism(4)
                   .writeTo(mongodb(dataConnectionRef("mongoDB"), defaultDatabase(), testName.getMethodName()))
                   .setLocalParallelism(2);

        JobConfig config = new JobConfig().setProcessingGuarantee(processingGuarantee).setSnapshotIntervalMillis(500);
        instance().getJet().newJob(pipeline, config).join();

        assertTrueEventually(() -> assertEquals(COUNT, collection.countDocuments()));
        assertEquals(HALF, collection.countDocuments(eq("type", "existing")));
        assertEquals(HALF, collection.countDocuments(eq("type", "new")));
    }

    @Test
    public void test_withStreamAsInput_insert() {
        final String connectionString = mongoContainer.getConnectionString();
        final String defaultDatabase = defaultDatabase();
        IMap<Long, Doc> mapToInsert = instance().getMap("toInsert");
        Doc doc = new Doc(null, 10L, "new", "text lorem ipsum etc");
        mapToInsert.put(10L, doc);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(newEntries(mapToInsert))
                .withIngestionTimestamps()
                .writeTo(builder(Doc.class, () -> createClient(connectionString))
                        .identifyDocumentBy("key", o -> o.key)
                        .checkResourceExistence(NEVER)
                        .into(i -> defaultDatabase, i -> "col_" + (i.key % 2))
                        .withCustomReplaceOptions(opt -> opt.upsert(true))
                        .writeMode(INSERT_ONLY)
                        .build()
                );

        mongo.getDatabase(defaultDatabase).createCollection("col_0");
        mongo.getDatabase(defaultDatabase).createCollection("col_1");

        JobConfig config = new JobConfig().setProcessingGuarantee(processingGuarantee).setSnapshotIntervalMillis(200);
        instance().getJet().newJob(pipeline, config);

        MongoCollection<Document> resultCollection = collection(defaultDatabase, "col_" + (doc.key % 2));
        assertTrueEventually(() -> {
            ArrayList<Document> list = resultCollection.find(eq("key", 10L)).into(new ArrayList<>());
            assertEquals(1, list.size());
            assertEquals("new", list.get(0).getString("type"));
        });
    }

    @Test
        public void test_withStreamAsInput_update() {
        MongoCollection<Doc> collection = collection(defaultDatabase(), testName.getMethodName())
                .withDocumentClass(Doc.class);

        IMap<Long, Doc> mapToUpdate = instance().getMap("toUpdate");
        Doc doc = new Doc(null, 3L, "existing", "text lorem ipsum etc");
        InsertOneResult res = collection.insertOne(doc);
        doc.id = res.getInsertedId().asObjectId().getValue();
        mapToUpdate.put(doc.key, doc);

        final String connectionString = mongoContainer.getConnectionString();
        final String defaultDatabase = defaultDatabase();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(newEntries(mapToUpdate)).withIngestionTimestamps()
                .map(item -> {
                    item.key *= 10;
                    return item;
                })
                .writeTo(builder(Doc.class, () -> createClient(connectionString))
                        .identifyDocumentBy("_id", o -> o.id)
                        .into(defaultDatabase, testName.getMethodName())
                        .withCustomReplaceOptions(opt -> opt.upsert(true))
                        .build()
                );

        JobConfig config = new JobConfig().setProcessingGuarantee(processingGuarantee).setSnapshotIntervalMillis(200);
        instance().getJet().newJob(pipeline, config);

        MongoCollection<Document> resultCollection = collection();
        assertTrueEventually(() -> {
            ArrayList<Document> list = resultCollection
                      .find(eq("_id", doc.id))
                      .into(new ArrayList<>());
            assertEquals(1, list.size());
            Document document = list.get(0);
            assertEquals(30L, document.get("key"));
        });
    }

    @Nonnull
    private static <T> StreamSource<T> newEntries(IMap<?, T> mapToInsert) {
        return mapJournal(mapToInsert, START_FROM_OLDEST,
                EventJournalMapEvent::getNewValue, e -> e.getType() == ADDED);
    }

    private static MongoClient createClient(String connectionString) {
        MongoClientSettings mcs = MongoClientSettings.builder()
                                                     .retryReads(true)
                                                     .retryWrites(true)
                                                     .applyConnectionString(new ConnectionString(connectionString))
                                                     .build();
        return MongoClients.create(mcs);
    }

    @SuppressWarnings("unused") // for serialization
    public static final class Doc implements Serializable {
        @BsonProperty("_id")
        private ObjectId id;
        private Long key;
        private String type;
        private String some;

        Doc() {
        }

        public Doc(ObjectId id, Long key, String type, String some) {
            this.id = id;
            this.key = key;
            this.type = type;
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

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
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
                    && Objects.equals(type, doc.type) && Objects.equals(some, doc.some);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, key, type, some);
        }
    }

    @Test
    public void test_whenServerNotAvailable() {
        String defaultDatabase = defaultDatabase();
        String collectionName = testName.getMethodName();
        Sink<Document> sink = MongoSinks
                .builder(Document.class, () -> mongoClient("non-existing-server", 0))
                .into(defaultDatabase, collectionName)
                .identifyDocumentBy("_id", (doc) -> doc.get("_id"))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(items(1, 2, 3))
         .map(i -> new Document("key", i))
         .writeTo(sink);

        try {
            instance().getJet().newJob(p).join();
            fail();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof JetException);
            assertContains(e.getCause().getMessage(), "Cannot connect to MongoDB");
        }
    }


}

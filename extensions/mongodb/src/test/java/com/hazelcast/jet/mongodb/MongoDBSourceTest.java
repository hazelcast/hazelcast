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

import com.google.common.collect.Sets;
import com.hazelcast.collection.IList;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.mongodb.MongoDBSourceBuilder.Batch;
import com.hazelcast.jet.mongodb.MongoDBSourceBuilder.Stream;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.mongodb.Mappers.streamToClass;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class MongoDBSourceTest extends AbstractMongoDBTest {

    @Parameters(name = "filter:{0} | projection: {1} | sort: {2} | map: {3}")
    public static Object[] filterProjectionSortMatrix() {
        Set<Boolean> booleans = new HashSet<>(asList(true, false));
        return Sets.cartesianProduct(booleans, booleans, booleans, booleans).stream()
                .map(tuple -> tuple.toArray(new Object[0]))
                .toArray(Object[]::new);
    }

    @Parameter(0)
    public boolean filter;
    @Parameter(1)
    public boolean projection;
    @Parameter(2)
    public boolean sort;
    @Parameter(3)
    public boolean map;

    @Test
    public void testBatchOneCollection() {
        IList<Object> list = instance().getList(testName.getMethodName());

        collection().insertMany(range(0, 30).mapToObj(i -> new Document("key", i).append("val", i)).collect(toList()));
        assertEquals(collection().countDocuments(), 30L);

        Pipeline pipeline = Pipeline.create();
        String connectionString = mongoContainer.getConnectionString();
        Batch<?> sourceBuilder = MongoDBSourceBuilder.batch(SOURCE_NAME, () -> mongoClient(connectionString))
                                                     .database(DB_NAME)
                                                     .collection(testName.getMethodName(), Document.class);
        sourceBuilder = batchFilters(sourceBuilder);
        pipeline.readFrom(sourceBuilder.build())
                .setLocalParallelism(2)
                .writeTo(Sinks.list(list));

        instance().getJet().newJob(pipeline, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE)).join();

        assertEquals(filter ? 20 : 30, list.size());
        if (map) {
            KV actual = (KV) list.get(0);
            if (projection) {
                assertNull(actual.key);
            }
            assertNotNull(actual.val);
        } else {
            Document actual = (Document) list.get(0);
            if (projection) {
                assertNull(actual.get("key"));
            }
            assertNotNull(actual.get("val"));
        }
    }

    @Test
    public void testBatchDatabase() {
        IList<Object> list = instance().getList(testName.getMethodName());

        collection().insertMany(range(0, 20).mapToObj(i -> new Document("key", i).append("val", i)).collect(toList()));
        assertEquals(collection().countDocuments(), 20L);

        collection(testName.getMethodName() + "_second")
                .insertMany(range(0, 20)
                        .mapToObj(i -> new Document("key", i).append("val", i).append("test", "other"))
                        .collect(toList()));
        assertEquals(collection().countDocuments(), 20L);

        Pipeline pipeline = Pipeline.create();
        String connectionString = mongoContainer.getConnectionString();
        Batch<?> sourceBuilder = MongoDBSourceBuilder.batch(SOURCE_NAME, () -> mongoClient(connectionString))
                .database(DB_NAME);
        sourceBuilder = batchFilters(sourceBuilder);
        pipeline.readFrom(sourceBuilder.build())
                .setLocalParallelism(2)
                .writeTo(Sinks.list(list));

        instance().getJet().newJob(pipeline, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE)).join();

        assertEquals(filter ? 20 : 40, list.size());
        if (map) {
            KV actual = (KV) list.get(0);
            if (projection) {
                assertNull(actual.key);
            }
            assertNotNull(actual.val);
        } else {
            Document actual = (Document) list.get(0);
            if (projection) {
                assertNull(actual.get("key"));
            }
            assertNotNull(actual.get("val"));
        }
    }

    private Batch<?> batchFilters(Batch<?> sourceBuilder) {
        if (filter) {
            sourceBuilder = sourceBuilder.filter(gte("key", 10));
        }
        if (projection) {
            sourceBuilder = sourceBuilder.project(include("val"));
        }
        if (sort) {
            sourceBuilder = sourceBuilder.sort(Sorts.ascending("val"));
        }
        if (map) {
            sourceBuilder = sourceBuilder.mapFn(Mappers.toClass(KV.class));
        }
        return sourceBuilder;
    }

    @Test
    public void testStreamOneCollection() {
        IList<Object> list = instance().getList(testName.getMethodName());
        String connectionString = mongoContainer.getConnectionString();
        Stream<?> sourceBuilder = MongoDBSourceBuilder.stream(SOURCE_NAME, () -> mongoClient(connectionString))
                                                      .database(DB_NAME)
                                                      .collection(testName.getMethodName(), Document.class);
        sourceBuilder = streamFilters(sourceBuilder);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(sourceBuilder.build())
                .withNativeTimestamps(0)
                .setLocalParallelism(1)
                .writeTo(Sinks.list(list));

        Job job = instance().getJet().newJob(pipeline);

        collection().insertOne(new Document("val", 1));
        collection().insertOne(new Document("val", 10).append("foo", "bar"));
        collection("someOther").insertOne(new Document("val", 1000).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(filter ? 1L : 2L, list.size());
//            Document document = list.get(0);
//            assertEquals(10, document.get("val"));
//            assertNull(document.get("foo"));
        });

        collection().insertOne(new Document("val", 2));
        collection().insertOne(new Document("val", 20).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(filter ? 2L : 4L, list.stream().distinct().count());
//            Document document = list.get(1);
//            assertEquals(20, document.get("val"));
//            assertNull(document.get("foo"));
        });
        job.cancel();
    }

    @Test
    public void testStream_whenWatchDatabase() {
        IList<Object> list = instance().getList(testName.getMethodName());

        String connectionString = mongoContainer.getConnectionString();

        Stream<?> builder = MongoDBSourceBuilder
                .stream(SOURCE_NAME, () -> MongoClients.create(connectionString))
                .database(DB_NAME);
        builder = streamFilters(builder);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(builder.build())
                .withNativeTimestamps(0)
                .setLocalParallelism(2)
                .writeTo(Sinks.list(list));

        instance().getJet().newJob(pipeline);

        MongoCollection<Document> col1 = collection("col1");
        MongoCollection<Document> col2 = collection("col2");

        col1.insertOne(new Document("val", 1));
        col1.insertOne(new Document("val", 10).append("foo", "bar"));

        col2.insertOne(new Document("val", 2));
        col2.insertOne(new Document("val", 11).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(filter ? 2 : 4, list.size());
//            list.forEach(document -> assertNull(document.get("foo")));
//
//            assertEquals(10, list.get(0).get("val"));
//            assertEquals(11, list.get(1).get("val"));

        });

        col1.insertOne(new Document("val", 3));
        col1.insertOne(new Document("val", 12).append("foo", "bar"));

        col2.insertOne(new Document("val", 4));
        col2.insertOne(new Document("val", 13).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(filter ? 4 : 8, list.size());
//            list.forEach(document -> assertNull(document.get("foo")));
//
//            assertEquals(12, list.get(2).get("val"));
//            assertEquals(13, list.get(3).get("val"));
        });

    }

    @Test
    public void testStream_whenWatchAll() {
        IList<Object> list = instance().getList("testStream_whenWatchAll");

        String connectionString = mongoContainer.getConnectionString();

        Stream<?> builder = MongoDBSourceBuilder.stream(SOURCE_NAME, () -> MongoClients.create(connectionString));
        builder = streamFilters(builder);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(builder.build())
                .withNativeTimestamps(0)
                .setLocalParallelism(2)
                .writeTo(Sinks.list(list));

        Job job = instance().getJet().newJob(pipeline);

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
            // note 7 here, not 6: it's because we have artificial db used in AbstractMongoDbTest
            assertEquals(filter ? 3 : 7, list.size());
//            list.forEach(document -> assertNull(document.get("foo")));

//            assertEquals(11, list.get(0).get("val"));
//            assertEquals(12, list.get(1).get("val"));
//            assertEquals(13, list.get(2).get("val"));
        });

        col1.insertOne(new Document("val", 4));
        col1.insertOne(new Document("val", 14).append("foo", "bar"));
        col2.insertOne(new Document("val", 5));
        col2.insertOne(new Document("val", 15).append("foo", "bar"));
        col2.insertOne(new Document("val", 6));
        col2.insertOne(new Document("val", 16).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(filter ? 6 : 13, list.size());
//            list.forEach(document -> assertNull(document.get("foo")));

//            assertEquals(14, list.get(3).get("val"));
//            assertEquals(15, list.get(4).get("val"));
//            assertEquals(16, list.get(5).get("val"));
        });

        job.cancel();
    }

    @Nonnull
    private Stream<?> streamFilters(Stream<?> sourceBuilder) {
        if (filter) {
            sourceBuilder = sourceBuilder.filter(and(
                    gte("fullDocument.val", 10),
                    eq("operationType", "insert")
            ));
        } else {
            sourceBuilder = sourceBuilder.filter(eq("operationType", "insert"));
        }
        if (projection) {
            sourceBuilder = sourceBuilder.project(include("fullDocument.val"));
        }
        if (map) {
            sourceBuilder = sourceBuilder.mapFn(streamToClass(KV.class));
        }
        sourceBuilder = sourceBuilder.startAtOperationTime(new BsonTimestamp(System.currentTimeMillis()));
        return sourceBuilder;
    }

    @SuppressWarnings("unused") // getters/setters are for Mongo converter
    public static class KV {
        private Integer key;
        private Integer val;
        public KV() {}

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public int getVal() {
            return val;
        }

        public void setVal(int val) {
            this.val = val;
        }
    }

}

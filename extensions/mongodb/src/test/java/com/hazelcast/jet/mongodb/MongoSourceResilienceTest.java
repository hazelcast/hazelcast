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
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.mongodb.AbstractMongoTest.TEST_MONGO_VERSION;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class MongoSourceResilienceTest extends SimpleTestInClusterSupport {
    private static final ILogger logger = Logger.getLogger(MongoSourceResilienceTest.class);

    @Rule
    public Network network = Network.newNetwork();
    @Rule
    public MongoDBContainer mongoContainer = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION)
            .withExposedPorts(27017)
            .withNetwork(network)
            .withNetworkAliases("mongo");

    @Rule
    public ToxiproxyContainer  toxi = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);

    @Rule
    public TestName testName = new TestName();

    private final Random random = new Random();

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();
    }

    @Test(timeout = 2 * 60_000)
    public void testWhenServerDown_graceful() {
        testWhenServerDown(true);
    }

    @Test(timeout = 2 * 60_000)
    public void testWhenServerDown_forceful() {
        testWhenServerDown(false);
    }

    void testWhenServerDown(boolean graceful) {
        Config conf = new Config();
        conf.getJetConfig().setEnabled(true);
        conf.addMapConfig(new MapConfig("*").setBackupCount(3));
        HazelcastInstance hz = createHazelcastInstance();
        HazelcastInstance serverToShutdown = createHazelcastInstance();
        JobRepository jobRepository = new JobRepository(hz);

        final String databaseName = "shutdownTest";
        final String collectionName = "testStream_whenServerDown";
        final String connectionString = mongoContainer.getConnectionString();
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            mongoClient.getDatabase(databaseName).createCollection(collectionName);
        }
        Pipeline pipeline = buildIngestPipeline(connectionString, "whenServerDown", databaseName, collectionName);

        Job job = invokeJob(hz, pipeline);
        IMap<Integer, Integer> set = hz.getMap("whenServerDown");
        AtomicInteger totalCount = new AtomicInteger(0);
        AtomicBoolean shouldContinue = new AtomicBoolean(true);

        spawnMongo(connectionString, mongoClient -> {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

            for (int i = 0; i < 20; i++) {
                collection.insertOne(new Document("key", i).append("source", "first"));
                totalCount.incrementAndGet();
                sleep(random.nextInt(100));
            }
        });

        waitForFirstSnapshot(jobRepository, job.getId(), 30, false);
        assertTrueEventually(() -> assertGreaterOrEquals("should have some records", set.size(), 1));
        spawnMongo(connectionString, mongoClient -> {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

            for (int i = 50; shouldContinue.get(); i++) {
                collection.insertOne(new Document("key", i));
                totalCount.incrementAndGet();
                sleep(random.nextInt(400));
            }
        });
        if (graceful) {
            serverToShutdown.shutdown();
        } else {
            serverToShutdown.getLifecycleService().terminate();
        }
        assertTrueEventually(() -> assertEquals(1, hz.getCluster().getMembers().size()));

        shouldContinue.set(false);

        assertTrueEventually(() -> assertEquals(totalCount.get(), set.size()));
    }

    @Test
    public void testNetworkCutoff() throws IOException {
        sourceNetworkTest(false);
    }

    @Test
    public void testNetworkTimeout() throws IOException {
        sourceNetworkTest(true);
    }

    private void sourceNetworkTest(boolean timeout) throws IOException {
        final String directConnectionString = mongoContainer.getConnectionString();
        HazelcastInstance hz = createHazelcastInstance();
        JobRepository jobRepository = new JobRepository(hz);
        createHazelcastInstance();

        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxi.getHost(), toxi.getControlPort());
        final Proxy proxy = toxiproxyClient.createProxy("mongo", "0.0.0.0:8670", "mongo:27017");

        final String databaseName = "networkCutoff";
        final String collectionName = "testNetworkCutoff";
        try (MongoClient mongoClient = MongoClients.create(directConnectionString)) {
            mongoClient.getDatabase(databaseName).createCollection(collectionName);
        }
        final String connectionViaToxi = "mongodb://" + toxi.getHost() + ":" + toxi.getMappedPort(8670);
        Pipeline pipeline = buildIngestPipeline(connectionViaToxi, "networkTest", databaseName, collectionName);

        Job job = invokeJob(hz, pipeline);
        IMap<Integer, Integer> set = hz.getMap("networkTest");

        final int itemCount = 20;
        AtomicInteger totalCount = new AtomicInteger(0);
        AtomicBoolean shouldContinue = new AtomicBoolean(true);

        spawnMongo(directConnectionString, mongoClient -> {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

            for (int i = 0; i < itemCount; i++) {
                collection.insertOne(new Document("key", i));
                totalCount.incrementAndGet();
            }
        });
        waitForFirstSnapshot(jobRepository, job.getId(), 30, false);

        spawnMongo(directConnectionString, mongoClient -> {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

            for (int i = itemCount; shouldContinue.get(); i++) {
                collection.insertOne(new Document("key", i));
                totalCount.incrementAndGet();
                sleep(300);
            }
        });
        logger.info("Injecting toxis");
        if (timeout) {
            proxy.toxics().timeout("TIMEOUT", UPSTREAM, 0);
            proxy.toxics().timeout("TIMEOUT_DOWNSTREAM", DOWNSTREAM, 0);
        } else {
            proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", DOWNSTREAM, 0);
            proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", UPSTREAM, 0);
        }
        sleep(3_000 + random.nextInt(1000));
        if (timeout) {
            proxy.toxics().get("TIMEOUT").remove();
            proxy.toxics().get("TIMEOUT_DOWNSTREAM").remove();
        } else {
            proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
            proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();
        }
        logger.info("Toxiing over");
        sleep(3_000 + random.nextInt(1000));
        shouldContinue.compareAndSet(true, false);

        assertTrueEventually(() -> assertEquals(totalCount.get(), set.size()));
    }

    @Nonnull
    private static Pipeline buildIngestPipeline(String mongoContainerConnectionString, String sinkName,
                                                String databaseName, String collectionName) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(MongoSourceBuilder
                        .stream("mongo source", () -> mongoClient(mongoContainerConnectionString))
                        .database(databaseName)
                        .collection(collectionName)
                        .mapFn((d, t) -> d.getFullDocument())
                        .startAtOperationTime(new BsonTimestamp(System.currentTimeMillis() / 1000))
                        .build())
                .withNativeTimestamps(0)
                .setLocalParallelism(4)
                .map(doc -> tuple2(doc.getInteger("key"), doc.getInteger("key")))
                .writeTo(map(sinkName));
        return pipeline;
    }


    @Nonnull
    private static Job invokeJob(HazelcastInstance hz, Pipeline pipeline) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(EXACTLY_ONCE)
                 .setSnapshotIntervalMillis(1000);
        Job job = hz.getJet().newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, RUNNING);
        return job;
    }

    private static void spawnMongo(String connectionString, Consumer<MongoClient> task) {
        FutureTask<Runnable> futureTask = new FutureTask<>(() -> {
            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                task.accept(mongoClient);
            }
        }, null);
        new Thread(futureTask).start();
    }
    private static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static MongoClient mongoClient(String connectionString) {
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .retryReads(false)
                .retryWrites(false)
                .applyToSocketSettings(builder -> {
                    builder.connectTimeout(2, SECONDS);
                    builder.readTimeout(2, SECONDS);
                })
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(2, SECONDS))
                .build();

        return MongoClients.create(settings);
    }

}

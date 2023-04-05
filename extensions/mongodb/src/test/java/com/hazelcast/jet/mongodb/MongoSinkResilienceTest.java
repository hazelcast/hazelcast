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
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
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
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.mongodb.AbstractMongoTest.TEST_MONGO_VERSION;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class MongoSinkResilienceTest extends SimpleTestInClusterSupport {
    private static final ILogger logger = Logger.getLogger(MongoSinkResilienceTest.class);

    @Rule
    public Network network = Network.newNetwork();

    @Rule
    public MongoDBContainer mongoContainer = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION)
            .withExposedPorts(27017)
            .withNetwork(network)
            .withNetworkAliases("mongo")
            ;

    @Rule
    public ToxiproxyContainer toxi = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);

    private final Random random = new Random();

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();
    }

    @Test(timeout = 2 * 60_000)
    public void testWhenMemberDown_graceful() {
        testWhenMemberDown(true);
    }

    @Test(timeout = 2 * 60_000)
    public void testWhenMemberDown_forceful() {
        testWhenMemberDown(false);
    }

    void testWhenMemberDown(boolean graceful) {
        Config conf = new Config();
        conf.addMapConfig(new MapConfig("*")
                .setEventJournalConfig(new EventJournalConfig().setEnabled(true))
                .setBackupCount(3)
        );
        conf.getJetConfig().setEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(conf);
        JobRepository jobRepository = new JobRepository(hz);
        HazelcastInstance memberToShutdown = createHazelcastInstance(conf);

        final String databaseName = "shutdownTest";
        final String collectionName = "testStream_whenServerDown";
        final String connectionString = mongoContainer.getConnectionString();
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            mongoClient.getDatabase(databaseName).createCollection(collectionName);
        }
        IMap<Integer, Integer> sourceMap = hz.getMap(randomName());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.mapJournal(sourceMap, START_FROM_OLDEST))
                .withIngestionTimestamps()
                .map(doc -> new Document("dummy", "test")
                        .append("key", doc.getKey())
                        .append("other", "" + doc.getValue())
                ).setLocalParallelism(2)
                .writeTo(MongoSinks.builder(Document.class, () -> mongoClient(connectionString))
                                   .into(databaseName, collectionName)
                                   .preferredLocalParallelism(2)
                                   .build());

        Job job = invokeJob(hz, pipeline);
        AtomicInteger totalCount = new AtomicInteger(0);
        AtomicBoolean continueCounting = new AtomicBoolean(true);

        spawn(() -> {
            while (continueCounting.get()) {
                int val = totalCount.incrementAndGet();
                sourceMap.put(val, val);
                sleep(random.nextInt(100));
            }
        });
        waitForFirstSnapshot(jobRepository, job.getId(), 30, true);
        sleep(500);

        if (graceful) {
            memberToShutdown.shutdown();
        } else {
            memberToShutdown.getLifecycleService().terminate();
        }
        assertTrueEventually(() -> assertEquals(1, hz.getCluster().getMembers().size()));

        continueCounting.set(false);
        MongoClient directClient = MongoClients.create(connectionString);
        MongoCollection<Document> collection = directClient.getDatabase(databaseName).getCollection(collectionName);
        assertTrueEventually(() ->
                assertEquals(totalCount.get(), collection.countDocuments())
        );
    }

    @Test
    public void testSink_networkCutoff() throws IOException {
        sinkNetworkTest(false);
    }

    @Test
    public void testSink_networkTimeout() throws IOException {
        sinkNetworkTest(true);
    }

    private void sinkNetworkTest(boolean timeout) throws IOException {
        Config conf = new Config();
        conf.addMapConfig(new MapConfig("*").setEventJournalConfig(new EventJournalConfig().setEnabled(true)));
        conf.getJetConfig().setEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(conf);
        JobRepository jobRepository = new JobRepository(hz);
        createHazelcastInstance(conf);

        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxi.getHost(), toxi.getControlPort());
        final Proxy proxy = toxiproxyClient.createProxy("mongo", "0.0.0.0:8670", "mongo:27017");

        final String connectionViaToxi = "mongodb://" + toxi.getHost() + ":" + toxi.getMappedPort(8670);
        final String databaseName = "networkCutoff";
        final String collectionName = "testNetworkCutoff";
        try (MongoClient mongoClient = MongoClients.create(connectionViaToxi)) {
            mongoClient.getDatabase(databaseName).createCollection(collectionName);
        }

        IMap<Integer, Integer> sourceMap = hz.getMap(randomName());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.mapJournal(sourceMap, START_FROM_OLDEST))
                .withIngestionTimestamps()
                .map(doc -> new Document("dummy", "test")
                        .append("key", doc.getKey())
                        .append("value", doc.getValue())
                ).setLocalParallelism(2)
                .writeTo(MongoSinks.builder(Document.class, () -> mongoClient(connectionViaToxi))
                                   .into(databaseName, collectionName)
                                   .preferredLocalParallelism(2)
                                   .build());

        Job job = invokeJob(hz, pipeline);
        final AtomicBoolean continueCounting = new AtomicBoolean(true);
        final AtomicInteger counter = new AtomicInteger(0);
        spawn(() -> {
            while (continueCounting.get()) {
                int val = counter.incrementAndGet();
                sourceMap.put(val, val);
                sleep(random.nextInt(100));
            }
        });
        waitForFirstSnapshot(jobRepository, job.getId(), 30, true);
        sleep(1_000);

        logger.info("Injecting toxis");
        if (timeout) {
            proxy.toxics().timeout("TIMEOUT", UPSTREAM, 0);
            proxy.toxics().timeout("TIMEOUT_DOWNSTREAM", DOWNSTREAM, 0);
        } else {
            proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", DOWNSTREAM, 0);
            proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", UPSTREAM, 0);
        }
        sleep(4_000 + random.nextInt(1000));
        if (timeout) {
            proxy.toxics().get("TIMEOUT").remove();
            proxy.toxics().get("TIMEOUT_DOWNSTREAM").remove();
        } else {
            proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
            proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();
        }
        logger.info("Toxiing over");
        sleep(1_000 + random.nextInt(1500));
        continueCounting.compareAndSet(true, false);

        final String directConnectionString = mongoContainer.getConnectionString();
        try (MongoClient directClient = MongoClients.create(directConnectionString)) {
            MongoCollection<Document> collection = directClient.getDatabase(databaseName).getCollection(collectionName);
            assertTrueEventually(() ->
                    assertEquals(counter.get(), collection.countDocuments())
            );
        }
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
                .applyToClusterSettings(b -> b.serverSelectionTimeout(5, SECONDS))
                .build();

        return MongoClients.create(settings);
    }

}

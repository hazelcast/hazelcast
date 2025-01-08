/*
 * Copyright 2025 Hazelcast Inc.
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
package com.hazelcast.jet.kafka.connect;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.TestedVersions.MONGO_VERSION;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.kafka.connect.TestUtil.getConnectorURL;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, NightlyTest.class})
public class KafkaConnectMongoDbIT extends JetTestSupport {
    private static MongoDBContainer mongoContainer;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void setupClass() {
        assumeDockerEnabled();
        mongoContainer = new MongoDBContainer("mongo:" + MONGO_VERSION);
        mongoContainer.start();
        mongoClient = MongoClients.create(mongoContainer.getConnectionString());
    }

    @Test
    public void clusterMemberDown() throws InterruptedException {
        Pipeline p = Pipeline.create();
        p.readFrom(streamSource("test"))
         .withIngestionTimestamps()
         .map(e -> Tuple2.tuple2(e, e))
         .writeTo(Sinks.map("test"))
         .getPipeline();


        Config config = smallInstanceConfig();
        config.setProperty("hazelcast.logging.type", "log4j2");
        config.getJetConfig().setResourceUploadEnabled(true);
        config.addMapConfig(new MapConfig("*").setBackupCount(2));
        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(config, 3);

        HazelcastInstance leader = hazelcastInstances[0];
        HazelcastInstance nonLeader = hazelcastInstances[2];

        var job = leader.getJet().newJob(p, jobConfig());
        var jobRepository = new JobRepository(leader);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        waitForNextSnapshot(jobRepository, job.getId(), 30, false);

        //Start creating new docs
        AtomicInteger recordsCreatedCounter = new AtomicInteger();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> scheduledFuture = scheduler.scheduleWithFixedDelay(() -> createOneRecord("test",
                recordsCreatedCounter.incrementAndGet()), 0, 10, MILLISECONDS);

        // wait for some snapshots and data
        SECONDS.sleep(3);
        waitForNextSnapshot(jobRepository, job.getId(), 30, false);

        Map<String, String> map = nonLeader.getMap("test");
        final int currentElements = map.size();
        System.out.printf("Created docs before scale down: %04d%n", recordsCreatedCounter.get());
        System.out.printf("Map size before scale down: %04d%n", currentElements);
        leader.shutdown();

        assertTrueEventually(() -> {
            Job j = nonLeader.getJet().getJob(job.getId());
            assert j != null;
            assertThat(j.getStatus()).isEqualTo(JobStatus.RUNNING);
        });

        // Wait for new elements
        assertTrueEventually(() -> assertThat(map).hasSizeGreaterThan(currentElements));

        scheduledFuture.cancel(false);

        assertTrueEventually(() -> assertThat(map).hasSize(recordsCreatedCounter.intValue()));
    }

    public JobConfig jobConfig() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(TestUtil.class);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        jobConfig.setSnapshotIntervalMillis(500);
        jobConfig.addJar(getConnectorURL("mongo-kafka-connect-1.10.0-all.jar"));
        return jobConfig;
    }

    public StreamSource<?> streamSource(String testName) {
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "mongo");
        connectorProperties.setProperty("connector.class", "com.mongodb.kafka.connect.MongoSourceConnector");
        connectorProperties.setProperty("connection.uri", mongoContainer.getConnectionString());
        connectorProperties.setProperty("database", testName);
        connectorProperties.setProperty("collection", testName);
        connectorProperties.setProperty("topic.prefix", "mongo");
        return KafkaConnectSources.connect(connectorProperties, TestUtil::convertToStringWithJustIndexForMongo);
    }

    public void createOneRecord(String testName, int index) {
        Document document = new Document().append("index", index);
        mongoClient.getDatabase(testName).getCollection(testName).insertOne(document);
    }

}

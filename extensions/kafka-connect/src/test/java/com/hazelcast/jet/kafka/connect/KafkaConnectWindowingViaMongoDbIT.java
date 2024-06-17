/*
 * Copyright 2024 Hazelcast Inc.
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

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.testcontainers.containers.MongoDBContainer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.kafka.connect.TestUtil.convertToType;
import static com.hazelcast.jet.kafka.connect.TestUtil.getConnectorURL;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, NightlyTest.class})
public class KafkaConnectWindowingViaMongoDbIT extends JetTestSupport {
    private static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "7.0.5");
    private static MongoDBContainer mongoContainer;
    private static MongoClient mongoClient;

    @Parameter(0)
    public int tasksMax;
    @Parameter(1)
    public int localParallelism;

    @Parameterized.Parameters(name = "tasksMax_{0}_localParallelism_{1}")
    public static Collection<Object[]> parameters() {
        // tasks.max, local parallelism
        // note: total parallelism = local * 2
        return Arrays.asList(new Object[][]{
                {1, 1},
                {2, 2},
                {3, 2},
                {4, 2}
        });
    }

    @BeforeClass
    public static void setupClass() {
        assumeDockerEnabled();
        mongoContainer = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);
        mongoContainer.start();
        mongoClient = MongoClients.create(mongoContainer.getConnectionString());
    }


    record ChangeStreamDoc(MyRecord fullDocument) {}

    record MyRecord(int index) {}

    @Test
    public void rollingCount() throws InterruptedException {
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        var instances = createHazelcastInstances(config, 2);
        final HazelcastInstance instance = instances[0];

        final String testName = randomName();
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "mongo");
        connectorProperties.setProperty("connector.class", "com.mongodb.kafka.connect.MongoSourceConnector");
        connectorProperties.setProperty("connection.uri", mongoContainer.getConnectionString());
        connectorProperties.setProperty("database", testName);
        connectorProperties.setProperty("collection", testName);
        connectorProperties.setProperty("topic.prefix", "mongo");
        connectorProperties.setProperty("tasks.max", String.valueOf(tasksMax));

        final IList<Long> testList = instance.getList(testName);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(KafkaConnectSources.connect(connectorProperties,
                        rec -> convertToType(rec, ChangeStreamDoc.class).fullDocument()))
                .withIngestionTimestamps()
                .setLocalParallelism(localParallelism)
                .window(WindowDefinition.tumbling(50))
                .distinct()
                .rollingAggregate(counting())
                .writeTo(Sinks.list(testList));
        JobConfig jobConfig = jobConfig();

        Job testJob = instance.getJet().newJob(pipeline, jobConfig);
        assertThat(testJob).eventuallyHasStatus(RUNNING);
        Thread.sleep(200);

        testList.clear();
        assertTrueEventually(() -> assertThat(testList).isEmpty());
        final AtomicLong recordsCreatedCounter = new AtomicLong();
        final int expectedSize = 9;
        for (int value = 0; value < expectedSize; value++) {
            createOneRecord(testName, value);
            recordsCreatedCounter.incrementAndGet();
        }

        assertTrueEventually(() -> assertThat(testList)
                .isNotEmpty()
                .last()
                .asInstanceOf(LONG)
                .isEqualTo(expectedSize)
        );
    }

    public JobConfig jobConfig() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(TestUtil.class);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        jobConfig.setSnapshotIntervalMillis(1500);
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

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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class KafkaConnectCouchbaseIT extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectCouchbaseIT.class);

    private static final String BUCKET_NAME = "mybucket";
    public static final CouchbaseContainer container = new CouchbaseContainer("couchbase/server:7.1.1")
            .withBucket(new BucketDefinition(BUCKET_NAME))
            .withStartupTimeout(Duration.ofSeconds(120))
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("Docker"))
            .withStartupAttempts(5);

    private static final int ITEM_COUNT = 100;

    private static final String COUCHBASE_LOGS_IN_CONTAINER = "/opt/couchbase/var/lib/couchbase/logs";
    private static final String COUCHBASE_LOGS_FILE = "couchbase-logs.tar.gz";

    @BeforeClass
    public static void setUpDocker() {
        assumeDockerEnabled();
        container.start();
    }

    @AfterClass
    public static void afterAll() throws Exception {
        if (container != null) {
            container.execInContainer("tar", "-czvf", "/tmp/" + COUCHBASE_LOGS_FILE, COUCHBASE_LOGS_IN_CONTAINER);
            container.copyFileFromContainer("/tmp/" + COUCHBASE_LOGS_FILE,
                    "target/" + COUCHBASE_LOGS_FILE);
            container.stop();
        }
    }


    @After
    public void clearDb() {
        try (Cluster cluster = connectToCluster()) {
            Bucket bucket = cluster.bucket(BUCKET_NAME);
            bucket.waitUntilReady(Duration.ofSeconds(10));

            CollectionManager collections = bucket.collections();
            for (ScopeSpec scopeSpec : collections.getAllScopes()) {
                for (CollectionSpec collectionSpec : scopeSpec.collections()) {
                    collections.dropCollection(scopeSpec.name(), collectionSpec.name());
                }
            }
        }
    }

    @Test
    public void testReading_1_1() throws Exception {
        testReading(1, 1);
    }

    @Test
    public void testReading_3_1() throws Exception {
        testReading(3, 1);
    }
    @Test
    public void testReading_3_2() throws Exception {
        testReading(3, 2);
    }

    @Test
    public void testReading_2_1() throws Exception {
        testReading(2, 1);
    }
    @Test
    public void testReading_2_2() throws Exception {
        testReading(2, 1);
    }


    public void testReading(int tasksMax, int localParallelism) throws Exception {
        Properties connectorProperties = new Properties();
        connectorProperties.put("tasks.max", String.valueOf(tasksMax));
        connectorProperties.setProperty("name", "couchbase");
        connectorProperties.setProperty("connector.class", "com.couchbase.connect.kafka.CouchbaseSourceConnector");
        connectorProperties.setProperty("couchbase.bucket", BUCKET_NAME);
        connectorProperties.setProperty("couchbase.seed.nodes", container.getConnectionString());
        connectorProperties.setProperty("couchbase.password", container.getPassword());
        connectorProperties.setProperty("couchbase.username", container.getUsername());
        connectorProperties.setProperty("couchbase.source.handler",
                "com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler");

        insertDocuments("items-1");

        Pipeline pipeline = Pipeline.create();
        StreamStage<Map<String, Object>> streamStage = pipeline
                .readFrom(KafkaConnectSources.connect(connectorProperties,
                        TestUtil::convertToString))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism)
                .map(base64 -> Base64.getDecoder().decode(base64))
                .map(JsonUtil::mapFrom);

        streamStage
                .writeTo(Sinks.list("results"));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getCouchbaseConnectorURL());


        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        LOGGER.info("Creating a job");

        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(config, 3);
        assertClusterSizeEventually(3, hazelcastInstances[0]);
        HazelcastInstance hazelcastInstance = hazelcastInstances[0];
        Job job = hazelcastInstance.getJet().newJob(pipeline, jobConfig);
        assertThat(job).eventuallyHasStatus(RUNNING);

        insertDocuments("items-2");

        //ITEM_COUNT is multiplied by 2 as there is insertDocuments before and after the pipeline
        assertTrueEventually(() -> assertThat(hazelcastInstance.getList("results"))
                                             .hasSize(ITEM_COUNT * 2));

        // double check it won't create new records
        Thread.sleep(Duration.ofSeconds(5).toMillis());
        assertThat(hazelcastInstance.getList("results")).hasSize(ITEM_COUNT * 2);
    }

    private static void insertDocuments(String collectionName) {
        try (Cluster cluster = connectToCluster()) {
            Bucket bucket = cluster.bucket(BUCKET_NAME);
            bucket.waitUntilReady(Duration.ofSeconds(10));

            LOGGER.info("Creating collection " + collectionName);
            CollectionManager collectionMgr = bucket.collections();
            CollectionSpec spec = CollectionSpec.create(collectionName);
            collectionMgr.createCollection(spec.scopeName(), collectionName);
            Collection collection = bucket.collection(collectionName);
            for (int i = 0; i < ITEM_COUNT; i++) {
                String id = collectionName + "-id-" + i;
                collection.insert(id, JsonObject.create().put("value", collectionName + "-value-" + i));
            }
        }
    }

    private static Cluster connectToCluster() {
        return Cluster.connect(container.getConnectionString(), container.getUsername(), container.getPassword());
    }

    private URL getCouchbaseConnectorURL() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        final String CONNECTOR_FILE_PATH = "couchbase-kafka-connect-couchbase-4.1.11.zip";
        URL resource = classLoader.getResource(CONNECTOR_FILE_PATH);
        assert resource != null;
        assertThat(new File(resource.toURI())).exists();
        return resource;
    }

}

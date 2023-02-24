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

package com.hazelcast.jet.kafka.connect;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.hazelcast.config.Config;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.kafka.connect.data.Values;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.net.URL;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({SlowTest.class, ParallelJVMTest.class})
public class KafkaConnectCouchbaseIntegrationTest extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    private static final ILogger LOGGER = Logger.getLogger(KafkaConnectCouchbaseIntegrationTest.class);
    private static final String BUCKET_NAME = "mybucket";

    @ClassRule
    public static final CouchbaseContainer container = new CouchbaseContainer("couchbase/server:7.1.1")
            .withBucket(new BucketDefinition(BUCKET_NAME));


    private static final int ITEM_COUNT = 1_000;

    private static final String CONNECTOR_URL = "https://repository.hazelcast.com/download"
            + "/tests/couchbase-kafka-connect-couchbase-4.1.11.zip";

    @Test
    public void testReadFromCouchbaseConnector() throws Exception {
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "couchbase");
        connectorProperties.setProperty("connector.class", "com.couchbase.connect.kafka.CouchbaseSourceConnector");
        connectorProperties.setProperty("tasks.max", "1");
        connectorProperties.setProperty("couchbase.bucket", BUCKET_NAME);
        connectorProperties.setProperty("couchbase.seed.nodes", container.getConnectionString());
        connectorProperties.setProperty("couchbase.password", container.getPassword());
        connectorProperties.setProperty("couchbase.username", container.getUsername());
        connectorProperties.setProperty("couchbase.source.handler",
                "com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler");

        insertDocuments("items-1");


        Pipeline pipeline = Pipeline.create();
        StreamStage<Map<String, Object>> streamStage = pipeline.readFrom(KafkaConnectSources.connect(connectorProperties))
                .withoutTimestamps()
                .map(record -> Values.convertToString(record.valueSchema(), record.value()))
                .map(base64 -> Base64.getDecoder().decode(base64))
                .map(JsonUtil::mapFrom);

        streamStage.writeTo(Sinks.logger());
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(2 * ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));


        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        LOGGER.info("Creating a job");
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, RUNNING);

        insertDocuments("items-2");

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    private static void insertDocuments(String collectionName) {
        try (Cluster cluster = connectToCluster()) {
            Bucket bucket = cluster.bucket(BUCKET_NAME);
            bucket.waitUntilReady(Duration.ofSeconds(10));

            LOGGER.info("Creating collection " + collectionName);
            CollectionManager collectionMgr = bucket.collections();
            CollectionSpec spec = CollectionSpec.create(collectionName);
            collectionMgr.createCollection(spec);
            Collection collection = bucket.collection(collectionName);
            for (int i = 0; i < ITEM_COUNT; i++) {
                String id = collectionName + "-id-" + i;
                LOGGER.info("Inserting document id=" + id + " into " + collectionName);
                collection.insert(id, JsonObject.create().put("value", collectionName + "-value-" + i));
            }
        }
    }

    private static Cluster connectToCluster() {
        return Cluster.connect(container.getConnectionString(), container.getUsername(), container.getPassword());
    }

}

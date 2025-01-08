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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.test.OverridePropertyRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.kafka.connect.KafkaConnectSources.connect;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaConnectScalingTest extends SimpleTestInClusterSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    public static final int ITEM_COUNT = 1_000;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);

        initialize(3, config);
    }

    @Test
    public void test_scaling() throws URISyntaxException {
        final int localParallelism = 3;
        final int tasksMax = 2 * localParallelism;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "orders");
        randomProperties.setProperty("quickstart", "orders");
        randomProperties.setProperty("tasks.max", String.valueOf(tasksMax)); // reduced from possible 3x

        Pipeline pipeline = Pipeline.create();
        StreamStage<KafkaConnectIT.Order> streamStage = pipeline
                .readFrom(connect(randomProperties, KafkaConnectIT.Order::new))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism);
        streamStage
                .writeTo(assertCollectedEventually(120, list -> assertThat(list).hasSize(ITEM_COUNT * tasksMax)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());


        // Create the collector before the job to make sure it is scheduled
        try (var collectors = new MultiNodeMetricsCollector<>(instances(), new KafkaConnectIT.KafkaMetricsCollector())) {
            try {
                Job job = instance().getJet().newJob(pipeline, jobConfig);
                job.join();
                fail("Job should have completed with an AssertionCompletedException, but completed normally");
            } catch (CompletionException e) {
                String errorMsg = e.getCause().getMessage();
                assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                        + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));

                assertTrueEventually(() -> assertThat(collectors.collector().getSourceRecordPollTotal()).isGreaterThan(ITEM_COUNT));
            }
        }
    }

    URL getDataGenConnectorURL() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        final String CONNECTOR_FILE_PATH = "confluentinc-kafka-connect-datagen-0.6.0.zip";
        URL resource = classLoader.getResource(CONNECTOR_FILE_PATH);
        assert resource != null;
        assertThat(new File(resource.toURI())).exists();
        return resource;
    }
}

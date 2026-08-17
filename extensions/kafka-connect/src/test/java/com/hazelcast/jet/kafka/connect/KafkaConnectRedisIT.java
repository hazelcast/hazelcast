/*
 * Copyright 2026 Hazelcast Inc.
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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.util.SetSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
@SerialTest
@SlowTest
@ParallelJVMTest
@SetSystemProperty(key = "hazelcast.logging.type", value = "log4j2")
public class KafkaConnectRedisIT extends JetTestSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectRedisIT.class);

    private static final String STREAM_NAME = "weather_sensor:wind";

    @Container
    public static final RedisContainer container = new RedisContainer(DockerImageName.parse("redis:6.2.6"))
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("Docker"));

    private static final int ITEM_COUNT = 1_000;

    @BeforeAll
    public static void setUpDocker() {
        assumeDockerEnabled();
    }

    @Test
    public void testReadFromRedisConnector() {
        insertData();

        Pipeline pipeline = Pipeline.create();

        Properties connectorProperties = getConnectorProperties();
        StreamSource<String> streamSource = KafkaConnectSources.connect(connectorProperties,
                TestUtil::convertToString);

        StreamStage<String> streamStage = pipeline.readFrom(streamSource)
                .withoutTimestamps()
                .setLocalParallelism(2);
        streamStage.writeTo(Sinks.logger());

        Sink<String> sink = AssertionSinks.assertCollectedEventually(60,
                list -> assertThat(list).hasSize(ITEM_COUNT));
        streamStage.writeTo(sink);

        JobConfig jobConfig = new JobConfig();

        Config config = smallInstanceConfig();
        // explicit false to ensure the connector on the classpath is used.
        config.getJetConfig().setResourceUploadEnabled(false);
        LOGGER.info("Creating a job");
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);
        assertThat(job).eventuallyHasStatus(RUNNING);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertThat(errorMsg)
                .withFailMessage("Job was expected to complete with AssertionCompletedException, but completed with: " + e.getCause())
                .contains(AssertionCompletedException.class.getName());
        }
    }

    @Nonnull
    private static Properties getConnectorProperties() {
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "RedisSourceConnector");
        connectorProperties.setProperty("connector.class", "com.redis.kafka.connect.RedisStreamSourceConnector");
        connectorProperties.setProperty("tasks.max", "2");
        connectorProperties.setProperty("redis.uri", container.getRedisURI());
        connectorProperties.setProperty("redis.stream.name", STREAM_NAME);

        return connectorProperties;
    }

    private void insertData() {
        String redisURI = container.getRedisURI();
        try (RedisClient client = RedisClient.create(redisURI);
             StatefulRedisConnection<String, String> connection = client.connect()) {

            RedisCommands<String, String> syncCommands = connection.sync();

            for (int index = 0; index < ITEM_COUNT; index++) {
                // Redis Streams messages are string key/values in Java.
                Map<String, String> messageBody = createMessageBody(index);

                String messageId = syncCommands.xadd(
                        STREAM_NAME,
                        messageBody);

                LOGGER.info("Message {} : {} posted", messageId, messageBody);
            }
        }
    }

    private Map<String, String> createMessageBody(int index) {
        Map<String, String> messageBody = new HashMap<>();
        messageBody.put("speed", String.valueOf(index));
        messageBody.put("direction", "270");
        messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));
        return messageBody;
    }
}

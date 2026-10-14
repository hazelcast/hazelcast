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

package com.hazelcast.jet.pulsar;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.bounce.BounceMemberExtension;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.pulsarDataConnectionConf;
import static com.hazelcast.jet.pulsar.PulsarSchema.string;
import static com.hazelcast.jet.pulsar.PulsarSources.pulsarReaderBuilder;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@SerialTest
@NightlyTest
public class PulsarE2EBounceTest extends PulsarTestSupport {
    private static final int ITEM_COUNT = 1_000;
    private static final int DURATION_SECONDS = 10 * 60;
    private final List<Closeable> closeables = new ArrayList<>();

    @RegisterExtension
    public BounceMemberExtension bounceMemberRule = BounceMemberExtension.with(config())
                                                                         .clusterSize(5)
                                                                         .driverCount(1)
                                                                         .useTerminate(true)
                                                                         .avoidOverlappingTerminations(true)
                                                                         .testTaskTimeoutSecs(150)
                                                                         .bouncingIntervalSeconds(10)
                                                                         .shouldFailOnIncompleteTask()
                                                                         .driverType(BounceTestConfiguration.DriverType.CLIENT)
                                                                         .build();

    @AfterEach
    void closeCloseables() {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception e) {
                logger.warning("Exception while closing resources", e);
            }
        }
        closeables.clear();
    }

    @Test
    @Timeout(value = 20, unit = MINUTES)
    public void test() throws Exception {
        bounceMemberRule.testRepeatedly(new Runnable[]{() -> {
            try {
                testWithUnstableCluster();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        }, DURATION_SECONDS);
    }

    private static Config config() {
        Config conf = smallInstanceConfig();
        conf.addMapConfig(new MapConfig("*").setBackupCount(1));
        conf.addDataConnectionConfig(pulsarDataConnectionConf("myPulsar", getServiceUrl(), pulsarContainer.getHttpServiceUrl(),
                                                              true));
        return conf;
    }

    private void testWithUnstableCluster() throws Exception {
        String inputTopic = randomName();
        String resultTopic = randomMapName();
        createTopic(inputTopic, true);
        createTopic(resultTopic, true);

        StreamSource<String> source = pulsarReaderBuilder(() -> Schema.BYTES)
                                          .topic(inputTopic)
                                          .projectionFn(message -> new String(message.getData(), StandardCharsets.UTF_8))
                                          .dataConnectionRef(dataConnectionRef("myPulsar"))
                                          .preferredLocalParallelism(2)
                                          .build();

        Sink<String> sink = PulsarSinks.builder(string())
                                       .topic(resultTopic)
                                       .dataConnectionRef(dataConnectionRef("myPulsar"))
                                       .inputType(String.class)
                                       .useTransactions(true)
                                       .transactionTimeout(Duration.ofSeconds(10))
                                       .extractValueFn(identity())
                                       .extractKeyFn(identity())
                                       .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .writeTo(sink);
        Job job = bounceMemberRule.getNextTestDriver().getJet().newJob(pipeline, new JobConfig()
                                                       .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                                                       .setSnapshotIntervalMillis(SECONDS.toMillis(1)));
        Producer<String> producer = getClient().newProducer(Schema.STRING)
                                               .topic(inputTopic)
                                               .sendTimeout(10, SECONDS)
                                               .create();
        closeables.add(producer);
        assertThat(job).eventuallyHasStatus(RUNNING);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        closeables.add(executor::shutdownNow);
        Future<Integer> publisher = executor.submit(() -> {
            int published = 0;
            for (int i = 0; i < ITEM_COUNT; i++) {
                producer.send("hello-pulsar-" + published);
                published++;
                sleepMillis(300);
            }
            return published;
        });

        int published = publisher.get(10, MINUTES);
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < published; i++) {
            String message = "hello-pulsar-" + i;
            expected.add(message);
        }
        Lock collectionLock = new ReentrantLock();
        List<String> result = new ArrayList<>();
        try (var topicReader = getClient().newReader(Schema.STRING)
                                          .startMessageId(MessageId.earliest)
                                          .subscriptionName(randomName())
                                          .topic(resultTopic)
                                          .readerListener((reader, msg) -> {
                                              String value = msg.getValue();
                                              collectionLock.lock();
                                              try {
                                                  result.add(value);
                                              } finally {
                                                  collectionLock.unlock();
                                              }
                                          })
                                          .create()) {
            closeables.add(topicReader);
            topicReader.hasMessageAvailable();

            assertTrueEventually(() -> {
                collectionLock.lock();
                try {
                    assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
                } finally {
                    collectionLock.unlock();
                }
                assertThat(result).hasSize(expected.size());
            });
            logger.info("Pulsar source churn: all " + published + " published messages received");
        }
    }

}

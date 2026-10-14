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
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.map.IMap;
import com.hazelcast.test.SerialTest;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.core.test.JetAssert.fail;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pulsar.PulsarSources.pulsarConsumerBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SerialTest
@SuppressWarnings("checkstyle:NestedTryDepth")
public class PulsarSourceConsumerTest extends PulsarTestSupport {
    private static final int ITEM_COUNT = 20;

    private final List<Closeable> closeables = new ArrayList<>();
    @AfterEach
    void closeCloseables() {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception e) {
                logger.warning("Exception while closing resources", e);
            }
        }
    }

    private static StreamSource<String> setupConsumerSource(String topicName,
                                                              FunctionEx<Message<byte[]>, String> projectionFn) {
        return pulsarConsumerBuilder(() -> Schema.BYTES)
                            .topic(topicName)
                            .projectionFn(projectionFn)
                            .connectionSupplier(() -> PulsarClient.builder().serviceUrl(getServiceUrl()).enableTransaction(true).build())
                            .subscriberName(randomName())
                            .preferredLocalParallelism(2)
                            .batchReceivePolicySupplier(() -> BatchReceivePolicy.DEFAULT_MULTI_TOPICS_DISABLE_POLICY)
                            .consumerCustomizer(consumer -> consumer.deadLetterPolicy(DeadLetterPolicy
                                                                                          .builder()
                                                                                          .deadLetterTopic("errors")
                                                                                          .maxRedeliverCount(40)
                                                                                          .build()))
                            .consumerConfig(Map.of())
                            .build();
    }

    @ParameterizedTest(name = "partitioned: {0}")
    @ValueSource(booleans = {true, false})
    public void when_projectionFunctionProvided_thenAppliedToReadRecords(boolean partitioned) {
        String topicName = randomName();
        createTopic(topicName, partitioned);
        // Add a suffix to messages so that this projectionFn does a bit more than byte->String conversion.
        StreamSource<String> pulsarConsumerSrc = setupConsumerSource(topicName,
                x -> new String(x.getData(), StandardCharsets.UTF_8) + "-suffix");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarConsumerSrc)
                .withoutTimestamps()
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertEquals("# of Emitted items should be equal to # of published items",
                                    ITEM_COUNT, list.size());
                            for (int i = 0; i < ITEM_COUNT; i++) {
                                String message = "hello-pulsar-" + i + "-suffix";
                                assertTrue("missing entry: " + message, list.contains(message));
                            }
                        })
                );
        Job job = createHazelcastInstance().getJet().newJob(pipeline);
        assertThat(job).eventuallyHasStatus(RUNNING);

        produceMessages("hello-pulsar", topicName, ITEM_COUNT);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Test
    public void when_readFromPulsarConsumer_then_jobGetsAllPublishedMessages() {
        HazelcastInstance[] instances = new HazelcastInstance[2];
        Arrays.setAll(instances, i -> createHazelcastInstance());

        String topicName = randomName();
        StreamSource<String> pulsarConsumerSrc = setupConsumerSource(topicName,
                x -> new String(x.getData(), StandardCharsets.UTF_8));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarConsumerSrc)
                .withoutTimestamps()
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertEquals("# of Emitted items should be equal to # of published items",
                                    ITEM_COUNT, list.size());
                            for (int i = 0; i < ITEM_COUNT; i++) {
                                String message = "hello-pulsar-" + i;
                                assertTrue("missing entry: " + message, list.contains(message));
                            }
                        })
                );
        Job job = instances[0].getJet().newJob(pipeline);
        assertThat(job).eventuallyHasStatus(RUNNING);

        produceMessages("hello-pulsar", topicName, ITEM_COUNT);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }

        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }

    @Test
    public void when_readUsingDataConnection_then_jobGetsAllPublishedMessages() {
        Config conf = smallInstanceConfig();
        conf.addDataConnectionConfig(new DataConnectionConfig("myPulsar")
                                         .setShared(true)
                                         .setType("Pulsar")
                                         .setProperty(PulsarDataConnection.Properties.BROKER_URL, getServiceUrl())
                                         .setProperty(PulsarDataConnection.Properties.HTTP_SERVICE_URL,
                                                      pulsarContainer.getHttpServiceUrl()));

        HazelcastInstance[] instances = new HazelcastInstance[2];
        Arrays.setAll(instances, i -> createHazelcastInstance(conf));

        String topicName = randomName();
        StreamSource<String> pulsarConsumerSrc = pulsarConsumerBuilder(() -> Schema.BYTES)
                                                     .topic(topicName)
                                                     .projectionFn(x -> new String(x.getData(), StandardCharsets.UTF_8))
                                                     .dataConnectionRef(dataConnectionRef("myPulsar"))
                                                     .subscriberName(randomName())
                                                     .build();

        IMap<Object, Object> resultMap = instances[0].getMap(randomMapName());
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarConsumerSrc)
                .withNativeTimestamps(0)
                .map(s -> Map.entry(s, s))
                .writeTo(Sinks.map(resultMap));
        Job job = instances[0].getJet().newJob(pipeline);
        assertThat(job).eventuallyHasStatus(RUNNING);

        produceMessages("hello-pulsar", topicName, ITEM_COUNT);

        assertTrueEventually(() -> {
            assertEquals("# of Emitted items should be equal to # of published items",
                         ITEM_COUNT, resultMap.size());
            for (int i = 0; i < ITEM_COUNT; i++) {
                String message = "hello-pulsar-" + i;
                assertTrue("missing entry: " + message, resultMap.containsKey(message));
            }
        });

        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }

    @Test
    public void integrationTest_noSnapshotting() {
        integrationTest(ProcessingGuarantee.NONE);
    }

    @Test
    public void integrationTest_withSnapshotting() {
        integrationTest(ProcessingGuarantee.EXACTLY_ONCE);
    }

    public void integrationTest(ProcessingGuarantee guarantee) {
        HazelcastInstance[] instances = new HazelcastInstance[2];
        Arrays.setAll(instances, i -> createHazelcastInstance());

        String topicName = randomName();
        StreamSource<String> pulsarReaderSrc = setupConsumerSource(topicName,
                                                                   x -> new String(x.getData(), StandardCharsets.UTF_8));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarReaderSrc)
                .withoutTimestamps()
                .rebalance()
                .writeTo(Sinks.list("test-list"));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(guarantee);
        jobConfig.setSnapshotIntervalMillis(SECONDS.toMillis(1));
        Job job = instances[0].getJet().newJob(pipeline, jobConfig);
        assertThat(job).eventuallyHasStatus(RUNNING);

        produceMessages("before-restart", topicName, ITEM_COUNT);

        Collection<Object> list = instances[0].getList("test-list");
        assertTrueEventually(() -> {
            assertThat(list).hasSize(ITEM_COUNT);
            for (int i = 0; i < ITEM_COUNT; i++) {
                String message = "before-restart-" + i;
                assertTrue("missing entry: " + message, list.contains(message));
            }
        }, 15);

        if (guarantee != ProcessingGuarantee.NONE) {
            // wait until a new snapshot appears
            JobRepository jr = new JobRepository(instances[0]);
            long currentMax = jr.getJobExecutionRecord(job.getId()).snapshotId();
            assertTrueEventually(() -> {
                JobExecutionRecord jobExecutionRecord = jr.getJobExecutionRecord(job.getId());
                assertThat(jobExecutionRecord).isNotNull();
                long newMax = jobExecutionRecord.snapshotId();
                assertTrue("no snapshot produced", newMax > currentMax);
                System.out.println("snapshot " + newMax + " found, previous was " + currentMax);
            });
            // Bring down one member. Job should restart and drain additional items (and maybe
            // some of the previous duplicately).
            Long lastExecutionId = assertThat(job).eventuallyJobRunning(instances[0], null);
            instances[1].getLifecycleService().terminate();
            assertThat(job).eventuallyJobRunning(instances[0], lastExecutionId);

            produceMessages("after-restart", topicName, ITEM_COUNT);

            List<String> expectedBefore = new ArrayList<>();
            for (int i = 0; i < ITEM_COUNT; i++) {
                String message = "before-restart-" + i;
                expectedBefore.add(message);
            }
            List<String> expectedAfter = new ArrayList<>();
            for (int i = 0; i < ITEM_COUNT; i++) {
                String message = "after-restart-" + i;
                expectedAfter.add(message);
            }
            assertTrueEventually(() -> {
                assertThat(list).containsOnlyOnceElementsOf(expectedBefore);
                assertThat(list).containsOnlyOnceElementsOf(expectedAfter);
                assertThat(list).hasSize(2 * ITEM_COUNT);
            }, 10);
        }
        assertFalse(job.getFuture().isDone());
        // cancel the job
        job.cancel();
        assertTrueEventually(() -> assertTrue(job.getFuture().isDone()));
    }

    @Test
    void consumerValidations() {
        SupplierEx<Schema<byte[]>> schemaSupplier = () -> Schema.BYTES;
        assertThatThrownBy(() -> PulsarSources.pulsarConsumerBuilder(schemaSupplier).subscriberName("test").build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("topics must not be null")
            .hasMessageContaining("Either connectionSupplier or dataConnectionRef must be provided");
        assertThatThrownBy(() -> PulsarSources.pulsarConsumerBuilder(schemaSupplier).connectionSupplier(notSerializableConnectionSupplier()).build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("\"connectionSupplier\" must be serializable");
    }
}

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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.IMap;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.pulsarDataConnectionConf;
import static com.hazelcast.jet.pulsar.PulsarSchema.json;
import static com.hazelcast.jet.pulsar.PulsarSources.pulsarReaderBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@SerialTest
@SlowTest
class PulsarSourceExactlyOnceTest extends PulsarTestSupport {

    enum Type implements Serializable {
        COUNT, ONLY_EMIT;
        @Serial
        private static final long serialVersionUID = 1L;
    }
    record Item (Type type, String value) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    @ParameterizedTest(name = "partitions: {0}")
    @ValueSource(ints = {0, 3, 12, 144})
    @Timeout(value = 10, unit = MINUTES)
    void restoresStateExactlyOnceAfterMemberLossAndScaleUp(int partitions) throws Exception {
        Config config = smallInstanceConfig();
        config.addDataConnectionConfig(pulsarDataConnectionConf("pulsar", getServiceUrl(),
                                                                pulsarContainer.getHttpServiceUrl(),
                                                                true));
        HazelcastInstance first = createHazelcastInstance(config);
        HazelcastInstance second = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        String topic = randomName();
        createTopic(topic, partitions == 0 ? null : partitions);
        var source = pulsarReaderBuilder(json(Item.class))
                         .topic(topic)
                         .dataConnectionRef(dataConnectionRef("pulsar"))
                         .preferredLocalParallelism(2)
                         .projectionFn(Message::getValue)
                         .build();
        IMap<String, Long> results = client.getMap(randomMapName());
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source).withoutTimestamps()
                .groupingKey(item -> item.value)
                .mapStateful(LongAccumulator::new, (count, key, item) -> {
                    if (item.type == Type.COUNT) {
                        count.add(1);
                        return null;
                    }
                    return Map.entry(key, count.get());
                })
                .writeTo(Sinks.map(results));
        Job job = client.getJet().newJob(pipeline, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(1000));
        try (Producer<Item> producer = getClient().newProducer(Schema.JSON(Item.class))
                                                    .topic(topic)
                                                    .batchingMaxMessages(100)
                                                    .batchingMaxPublishDelay(10, MILLISECONDS)
                                                    .sendTimeout(10, SECONDS)
                                                    .create()) {
            long execution = assertThat(job).eventuallyJobRunning(first, null);
            publish(producer, Type.COUNT, 0, 100);
            assertTrueEventually(() -> assertThat(new JobRepository(first).getJobExecutionRecord(job.getId()).snapshotId())
                    .isGreaterThanOrEqualTo(0));

            publish(producer, Type.COUNT, 100, 1000);
            second.getLifecycleService().terminate();
            assertClusterSizeEventually(1, first);
            execution = assertThat(job).eventuallyJobRunning(first, execution);
            second = createHazelcastInstance(config);
            assertClusterSizeEventually(2, first, second);
            execution = assertThat(job).eventuallyJobRunning(first, execution);
            waitAllForSafeState(first, second);

            publish(producer, Type.COUNT, 1000, 2000);
            first.getLifecycleService().terminate();
            assertThat(job).eventuallyJobRunning(second, execution);

            // check ignored messages won't mess up the state; in case of bug hidden by IMap idempotency,
            // this will override the result to incorrect one
            publish(producer, Type.ONLY_EMIT, 0, 2000);
            Map<String, Long> expected = new HashMap<>();
            for (int i = 0; i < 2000; i++) {
                expected.put(Integer.toString(i), 1L);
            }
            assertTrueEventually(() -> assertThat((Map<String, Long>) results).containsAllEntriesOf(expected));
        } finally {
            job.cancel();
        }
    }

    private static void publish(Producer<Item> producer, Type type, int first, int limit) throws Exception {
        var futures = new ArrayList<CompletableFuture<?>>();
        for (int i = first; i < limit; i++) {
            futures.add(producer.newMessage()
                                .key(Integer.toString(i))
                                .value(new Item(type, Integer.toString(i)))
                                .sendAsync());
        }
        producer.flush();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .get(30, SECONDS);
    }
}

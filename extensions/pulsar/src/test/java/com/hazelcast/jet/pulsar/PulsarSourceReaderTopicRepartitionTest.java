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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;
import com.hazelcast.test.SerialTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pulsar.PulsarSources.pulsarReaderBuilder;

@SerialTest
public class PulsarSourceReaderTopicRepartitionTest extends PulsarTestSupport {
    private static final int ITEM_COUNT = 9000;
    private final AtomicInteger counter = new AtomicInteger(0);

    @CartesianTest
    public void when_topicRepartition_then_readsAllCorrectly(
        @CartesianTest.Values(booleans = {false, true}) boolean suspend,
        @CartesianTest.Values(ints = {1, 3}) int topicCount
    ) throws PulsarClientException, PulsarAdminException {
        Config conf = smallInstanceConfig();
        conf.addDataConnectionConfig(new DataConnectionConfig("myPulsar")
                                         .setShared(true)
                                         .setType("Pulsar")
                                         .setProperty(PulsarDataConnection.Properties.BROKER_URL, getServiceUrl())
                                         .setProperty(PulsarDataConnection.Properties.HTTP_SERVICE_URL,
                                                      pulsarContainer.getHttpServiceUrl()));

        HazelcastInstance[] instances = new HazelcastInstance[2];
        Arrays.setAll(instances, i -> createHazelcastInstance(conf));

        String[] topics = new String[topicCount];
        Arrays.setAll(topics, i -> randomName());
        for (String topic : topics) {
            createTopic(topic, 3);
        }

        StreamSource<String> pulsarConsumerSrc = pulsarReaderBuilder(() -> Schema.BYTES)
                                                     .topic(topics)
                                                     .projectionFn(x -> new String(x.getData(), StandardCharsets.UTF_8))
                                                     .dataConnectionRef(dataConnectionRef("myPulsar"))
                                                     .readerCustomizer(rb -> rb.readerName("myReader"))
                                                     .build();

        IMap<Object, Object> resultMap = instances[0].getMap(randomMapName());
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarConsumerSrc)
                .withNativeTimestamps(0)
                .map(s -> Map.entry(s, s))
                .writeTo(Sinks.map(resultMap));
        Job job = instances[0].getJet().newJob(pipeline, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
        assertThat(job).eventuallyHasStatus(RUNNING);
        sleepSeconds(5);

        for (int i = 0; i < topicCount; i++) {
            produceMessages(topics[i], ITEM_COUNT / topicCount);
        }

        if (suspend) {
            job.suspend();
        }

        for (int i = 0; i < topicCount; i++) {
            changeNumberOfPartitions(topics[i], 5 + 2 * i);
        }

        if (suspend) {
            job.resume();
        }
        sleepSeconds(5);
        for (int i = 0; i < topicCount; i++) {
            produceMessages(topics[i], ITEM_COUNT / topicCount);
        }

        assertTrueEventually(() -> {
            assertEquals("# of Emitted items should be equal to # of published items",
                         ITEM_COUNT * 2, resultMap.size());
            for (int i = 0; i < ITEM_COUNT * 2; i++) {
                String message = "hello-pulsar-" + i;
                assertTrue("missing entry: " + message, resultMap.containsKey(message));
            }
        });
    }

    private static void changeNumberOfPartitions(String topicName, int newPartitionCount)
        throws PulsarAdminException, PulsarClientException {
        try (PulsarAdmin admin = PulsarAdmin.builder()
                                            .serviceHttpUrl(pulsarContainer.getHttpServiceUrl())
                                            .build()) {
            admin.topics().updatePartitionedTopic(topicName, newPartitionCount);
        }
    }

    protected void produceMessages(String topicName, int count) {
        for (int i = 0; i < count; i++) {
            String message = "hello-pulsar-" + counter.getAndIncrement();
            try {
                getProducer(topicName)
                    .newMessage()
                    .key(String.valueOf(i))
                    .value(message.getBytes(StandardCharsets.UTF_8))
                    .send();
            } catch (PulsarClientException e) {
                sneakyThrow(e);
            }
        }
    }
}

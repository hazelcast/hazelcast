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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.BROKER_URL;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.ENABLE_TRANSACTIONS;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.HTTP_SERVICE_URL;
import static com.hazelcast.jet.pulsar.PulsarSchema.json;
import static com.hazelcast.jet.pulsar.PulsarUtils.newReader;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@SerialTest
@NightlyTest
@SlowTest
public class PulsarSinkFailureTest extends PulsarTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSinkFailureTest.class);
    private TestHazelcastFactory instanceFactory;
    private HazelcastInstance[] instances;

    private String dataConnectionName;
    private String topicName;

    @BeforeEach
    void startHazelcast() {
        instanceFactory = new TestHazelcastFactory();
        Config conf = smallInstanceConfig();
        conf.addMapConfig(new MapConfig("*")
                              .setEventJournalConfig(new EventJournalConfig().setEnabled(true)));
        instances = instanceFactory.newInstances(conf, 3);

        dataConnectionName = randomName();
        topicName = randomMapName();
        instances[0].getConfig()
                    .addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                                                 .setShared(true)
                                                 .setType("Pulsar")
                                                 .setProperty(BROKER_URL, pulsarContainer.getPulsarBrokerUrl())
                                                 .setProperty(HTTP_SERVICE_URL, pulsarContainer.getHttpServiceUrl())
                                                 .setProperty(ENABLE_TRANSACTIONS, "true"));
    }

    @AfterEach
    void stopHazelcast() {
        for (HazelcastInstance instance : instances) {
            if (instance != null) {
                instance.getLifecycleService().terminate();
            }
        }
        instanceFactory.terminateAll();
        instanceFactory = null;
    }

    public static ProcessedData toData(Map.Entry<String, String> e) {
        return new ProcessedData(e.getKey(), e.getValue());
    }

    @Test
    public void survivesMemberDown() throws IOException {
        createTopic(topicName, true);
        Sink<Map.Entry<String, String>> pulsarSink = PulsarSinks.builder(json(ProcessedData.class), PulsarSinkFailureTest::toData)
                                                                .dataConnectionRef(dataConnectionRef(dataConnectionName))
                                                                .topic(topicName)
                                                                .transactionTimeout(Duration.ofSeconds(10))
                                                                .preferredLocalParallelism(2)
                                                                .producerCustomizer(builder ->
                                                                                        builder.sendTimeout(1, MINUTES))
                                                                .build();

        IMap<String, String> input = instances[0].getMap("inputForSurvivesMemberDown");
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(input, JournalInitialPosition.START_FROM_OLDEST))
         .withIngestionTimestamps()
         .writeTo(pulsarSink);

        var conf = new JobConfig();
        conf.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        conf.setSnapshotIntervalMillis(100);
        var job = instances[0].getJet().newJob(p, conf);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        var msgProducer = new MessageProducer(input);
        msgProducer.start();

        sleepSeconds(3);
        assertTrueEventually(() -> assertThat(msgProducer.getCounter()).isGreaterThanOrEqualTo(20));
        sleepSeconds(2);

        instances[2].getLifecycleService().terminate();
        instances[2] = null;
        sleepSeconds(10);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        msgProducer.stop();
        int finalCount = msgProducer.getCounter();
        List<ProcessedData> expected = new ArrayList<>(finalCount);
        for (int i = 1; i <= finalCount; i++) {
            expected.add(new ProcessedData(String.valueOf(i), String.valueOf(i)));
        }

        Lock collectionLock = new ReentrantLock();
        List<ProcessedData> results = Collections.synchronizedList(new ArrayList<>());
        try (var reader = newReader(topicName, Schema.JSON(ProcessedData.class), results, collectionLock)) {
            reader.hasMessageAvailable();
            assertTrueEventually(() -> {
                collectionLock.lock();
                ArrayList<ProcessedData> copy;
                try {
                    copy = new ArrayList<>(results);
                } finally {
                    collectionLock.unlock();
                }
                List<ProcessedData> sorted = copy.stream().sorted().toList();
                assertThat(sorted).containsExactlyInAnyOrderElementsOf(expected);
            });
        }
        job.cancel();
    }

    record ProcessedData(String name, String value) implements Serializable, Comparable<ProcessedData> {
        @Override
        public int compareTo(PulsarSinkFailureTest.ProcessedData o) {
            int nameCompare = name.compareTo(o.name);
            if (nameCompare != 0) {
                return nameCompare;
            }
            return value.compareTo(o.value);
        }
    }

    private static class MessageProducer {
        private final IMap<String, String> imap;
        private final ScheduledExecutorService executor;
        private final AtomicInteger counter = new AtomicInteger();

        private MessageProducer(IMap<String, String> imap) {
            this.imap = imap;
            executor = Executors.newSingleThreadScheduledExecutor();
        }

        int getCounter() {
            return counter.get();
        }

        void start() {
            executor.scheduleAtFixedRate(() -> {
                for (int i = 0; i < 2; i++) {
                    int value = counter.incrementAndGet();
                    imap.set(String.valueOf(value), String.valueOf(value));
                }
            }, 1, 1, SECONDS);
        }

        void stop() {
            LOG.info("MessageProducer stopped");
            executor.shutdownNow();
        }
    }
}

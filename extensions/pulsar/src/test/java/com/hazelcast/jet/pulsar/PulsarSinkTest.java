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
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ThrowingRunnable;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.toxic.Bandwidth;
import org.apache.pulsar.client.api.PulsarClient;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.jet.TestedVersions.TOXIPROXY_IMAGE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.PulsarContainer.BROKER_PORT;


public class PulsarSinkTest extends PulsarTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSinkTest.class);
    private static final int ITEM_COUNT = 1_000;
    static TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
    static HazelcastInstance[] instances;

    public ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("toxiproxy");

    private Job job;

    @BeforeAll
    static void startHazelcast() {
        Config conf = smallInstanceConfig();
        conf.addMapConfig(new MapConfig("*").setEventJournalConfig(new EventJournalConfig().setEnabled(true)));
        instances = instanceFactory.newInstances(conf, 2);
    }

    @AfterAll
    static void stopHazelcast() {
        instanceFactory.terminateAll();
    }

    @AfterEach
    void tearDown() {
        closeResource(toxiproxy);
        if (instances != null) {
            instances[0].getJet().getJobs().forEach(Job::cancel);
        }
        if (job != null) {
            try {
                job.cancel();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void happyPath() throws PulsarClientException {
        String topicName = randomName();
        Sink<Integer> pulsarSink = setupSink(topicName); // Its projection function -> Integer::doubleValue

        Pipeline p = Pipeline.create();
        List<Integer> numbers = IntStream.range(0, ITEM_COUNT).boxed().toList();
        p.readFrom(TestSources.items(numbers))
         .writeTo(pulsarSink);

        job = instances[0].getJet().newJob(p);
        job.join();
        List<Double> list = consumeMessages(topicName, ITEM_COUNT);

        assertTrueEventually(() -> {
            assertThat(list).hasSize(ITEM_COUNT);
            for (double i = 0; i < ITEM_COUNT; i++) {
                assertTrue("missing entry: " + i, list.contains(i));
            }
        }, 10);
    }

    @ParameterizedTest
    @ValueSource(booleans =  {true, false})
    public void serviceUnavailable(boolean scenarioEndingInError) throws IOException {
        final int durationOfToxicsSec = scenarioEndingInError ? 60 : 2;
        final String topicName = randomName();
        final String mapName = randomMapName();

        toxiproxy.start();
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        Proxy proxy = toxiproxyClient.createProxy("toxiproxy", "0.0.0.0:" + 8678,
                                                  BROKER_HOST + ":" + BROKER_PORT);
        final String toxicUrl = "pulsar://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8678);

        IMap<String, Integer> inputMap = instances[0].getMap(mapName);
        var producer = new MessageProducer(inputMap);
        producer.start();

        var pipeline = Pipeline.create();
        pipeline.readFrom(Sources.mapJournal(inputMap, JournalInitialPosition.START_FROM_OLDEST))
                .withIngestionTimestamps()
                .map(Map.Entry::getValue)
                .writeTo(PulsarSinks.builder(() -> Schema.DOUBLE, Integer::doubleValue)
                             .topic(topicName)
                             .connectionSupplier(() -> PulsarClient.builder()
                                                      .serviceUrl(toxicUrl)
                                                      .connectionTimeout(1, SECONDS)
                                                      .operationTimeout(1, SECONDS)
                                                      .build())
                             .producerConfig(Map.of("sendTimeoutMs", 2500))
                             .build());

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        job = instances[0].getJet().newJob(pipeline, config);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        job.addStatusListener(event -> {
            if (event.getNewStatus() == JobStatus.RUNNING) {
                LOG.info("Job is running");
                executor.submit(ThrowingRunnable.wrap(() -> {
                    sleepMillis(500);
                    // inject toxics after initialization; it will always fail if service is unavailable during init
                    Bandwidth bandwidth = proxy.toxics().bandwidth("bandwidth", UPSTREAM, 0);
                    Bandwidth bandwidth2 = proxy.toxics().bandwidth("bandwidthD", DOWNSTREAM, 0);
                    LOG.info("Toxics injected");

                    sleepSeconds(durationOfToxicsSec);

                    bandwidth.remove();
                    bandwidth2.remove();
                    LOG.info("Toxics removed");
                }));
            }
        });

        sleepSeconds(3);
        producer.stop();

        if (scenarioEndingInError) {
            assertTrueEventually(() -> assertThat(job).eventuallyHasStatus(JobStatus.FAILED));
        } else {
            List<Double> resultList = Collections.synchronizedList(new ArrayList<>());
            try (var client = PulsarClient.builder()
                                          .serviceUrl(getServiceUrl())
                                          .build();
                 var topicReader = client.newReader(Schema.DOUBLE)
                                .startMessageFromRollbackDuration(1, MINUTES)
                                .subscriptionName(randomName())
                                .topic(topicName)
                                .readerListener((reader, msg) -> {
                                    LOG.info("Message received: {}, redelivered: {}", msg.getValue(), msg.getRedeliveryCount());
                                    resultList.add(msg.getValue());
                                })
                                .create()) {
                topicReader.hasMessageAvailable();
                final int size = inputMap.size();
                LOG.info("All produced items: {}", inputMap);
                assertTrueEventually(() -> assertThat(resultList).hasSize(size), 60);
            }
        }
    }

    @Test
    void validations() {
        assertThatThrownBy(() -> PulsarSinks.builder(() -> Schema.DOUBLE).build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("topic must not be null")
            .hasMessageContaining("Either connectionSupplier or dataConnectionRef must be provided");
        assertThatThrownBy(() -> PulsarSinks.builder(() -> Schema.DOUBLE).connectionSupplier(notSerializableConnectionSupplier()).build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("\"connectionSupplier\" must be serializable");
    }

    private static class MessageProducer {
        private final IMap<String, Integer> imap;
        private final ScheduledExecutorService executor;
        private final AtomicInteger counter = new AtomicInteger();

        private MessageProducer(IMap<String, Integer> imap) {
            this.imap = imap;
            executor = Executors.newSingleThreadScheduledExecutor();
        }

        void start() {
            executor.scheduleAtFixedRate(() -> {
                for (int i = 0; i < 2; i++) {
                    int value = counter.incrementAndGet();
                    imap.set(String.valueOf(value), value);
                }
            }, 1, 1, SECONDS);
        }

        void stop() {
            LOG.info("MessageProducer stopped");
            executor.shutdownNow();
        }
    }
}

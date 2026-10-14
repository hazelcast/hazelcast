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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ThrowingRunnable;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobAssertions;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import com.hazelcast.test.LogEntryMatcher;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.NightlyTest;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.toxic.Bandwidth;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.jet.TestedVersions.TOXIPROXY_IMAGE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.BROKER_URL;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.ENABLE_TRANSACTIONS;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.HTTP_SERVICE_URL;
import static com.hazelcast.jet.pulsar.PulsarSchema.string;
import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.PulsarContainer.BROKER_PORT;

@SerialTest
public class PulsarSinkTest extends PulsarTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSinkTest.class);
    private static final int ITEM_COUNT = 100;

    final TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
    HazelcastInstance[] instances;

    public final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("toxiproxy");

    private Job job;
    private String dataConnectionName;

    @BeforeEach
    void startHazelcast() {
        Config conf = smallInstanceConfig();
        conf.addMapConfig(new MapConfig("*")
                              .setEventJournalConfig(new EventJournalConfig().setEnabled(true)));
        instances = instanceFactory.newInstances(conf, 2);
        dataConnectionName = randomName();
        instances[0].getConfig()
                    .addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                                                 .setShared(true)
                                                 .setType("Pulsar")
                                                 .setProperty(BROKER_URL, pulsarContainer.getPulsarBrokerUrl())
                                                 .setProperty(HTTP_SERVICE_URL, pulsarContainer.getHttpServiceUrl())
                                                 .setProperty(ENABLE_TRANSACTIONS, "true"))
        ;
    }

    @AfterEach
    void stopHazelcast() {
        closeResource(toxiproxy);
        instanceFactory.terminateAll();
    }

    @Test
    public void happyPath() throws IOException {
        String topicName = randomName();
        createTopic(topicName, true);
        Sink<Integer> pulsarSink = PulsarSinks.builder(() -> Schema.DOUBLE)
                                       .topic(topicName)
                                       .connectionSupplier(() -> PulsarClient.builder()
                                                                            .serviceUrl(getServiceUrl())
                                                                            .enableTransaction(true)
                                                                            .build())
                                       .httpServiceUrl(pulsarContainer.getHttpServiceUrl())
                                       .inputType(Integer.class)
                                       .extractValueFn(Integer::doubleValue)
                                       .build();

        Pipeline p = Pipeline.create();
        List<Integer> numbers = IntStream.range(0, ITEM_COUNT).boxed().toList();
        p.readFrom(TestSources.items(numbers))
         .writeTo(pulsarSink);

        var conf = new JobConfig();
        conf.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        job = instances[0].getJet().newJob(p, conf);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        List<Double> results = Collections.synchronizedList(new ArrayList<>());
        try (var reader = PulsarUtils.newReader(topicName, Schema.DOUBLE, results)) {
            reader.hasMessageAvailable();
            assertTrueEventually(() -> {
                assertThat(results).hasSize(ITEM_COUNT);
                for (double i = 0; i < ITEM_COUNT; i++) {
                    assertTrue("missing entry: " + i, results.contains(i));
                }
            }, 20);
        }
    }

    @Test
    public void happyPathNoTransactions() throws IOException {
        String topicName = randomName();
        createTopic(topicName, true);
        Sink<Integer> pulsarSink = PulsarSinks.builder(PulsarSchema.doubles())
                                              .connectionSupplier(() -> PulsarClient.builder()
                                                                                    .serviceUrl(getServiceUrl())
                                                                                    .build())
                                              .inputType(Integer.class)
                                              .topic(topicName)
                                              .httpServiceUrl(pulsarContainer.getHttpServiceUrl())
                                              .extractValueFn(Integer::doubleValue)
                                              .useTransactions(false)
                                              .build();

        Pipeline p = Pipeline.create();
        List<Integer> numbers = IntStream.range(0, ITEM_COUNT).boxed().toList();
        p.readFrom(TestSources.items(numbers))
         .writeTo(pulsarSink);

        var conf = new JobConfig();
        conf.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        job = instances[0].getJet().newJob(p, conf);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        List<Double> results = Collections.synchronizedList(new ArrayList<>());
        try (var reader = PulsarUtils.newReader(topicName, Schema.DOUBLE, results)) {
            reader.hasMessageAvailable();
            assertTrueEventually(() -> {
                assertThat(results).hasSize(ITEM_COUNT);
                for (double i = 0; i < ITEM_COUNT; i++) {
                    assertTrue("missing entry: " + i, results.contains(i));
                }
            }, 20);
        }
    }

    record ProcessedData(String key, String value, long timestamp) {
    }

    @Test
    public void partitionedWriting() throws IOException {
        String topicName = randomName();
        createTopic(topicName, true);

        Pipeline p = Pipeline.create();
        IMap<String, String> inputMap = instances[0].getMap(randomMapName());

        p.readFrom(Sources.map(inputMap))
         .map(e -> new ProcessedData(e.getKey(), e.getValue(), System.currentTimeMillis()))
         .writeTo(PulsarSinks.builder(string())
                             .topic(topicName)
                             .dataConnectionRef(dataConnectionRef(dataConnectionName))
                             .inputType(ProcessedData.class)
                             .extractKeyFn(e -> e.key)
                             .extractValueFn(e -> e.key + "/" + e.value)
                             .extractTimestampFn(e -> e.timestamp)
                             .extractPropertiesFn(e -> Map.of())
                             .httpServiceUrl(pulsarContainer.getHttpServiceUrl())
                             .build()
         );

        List<String> expectedResult = new ArrayList<>(ITEM_COUNT);
        for (int i = 0; i < ITEM_COUNT; i++) {
            inputMap.put(String.valueOf(i), String.valueOf(i % 10));
            expectedResult.add(i + "/" + (i % 10));
        }

        var conf = new JobConfig();
        conf.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        job = instances[0].getJet().newJob(p, conf);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.COMPLETED);

        List<String> results = Collections.synchronizedList(new ArrayList<>());
        try (var reader = PulsarUtils.newReader(topicName, Schema.STRING, results)) {
            reader.hasMessageAvailable();
            assertTrueEventually(() -> assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResult));
        }
    }

    @NightlyTest
    @Timeout(value = 25, unit = MINUTES)
    @CartesianTest(name = "{index} - scenarioEndingInError={0} - processingGuarantee={1}")
    public void serviceUnavailable(
        @CartesianTest.Values(booleans = { false, true }) boolean scenarioEndingInError,
        @CartesianTest.Enum(value = ProcessingGuarantee.class, names = { "EXACTLY_ONCE", "AT_LEAST_ONCE" }) ProcessingGuarantee processingGuarantee
    ) throws IOException {
        final int durationOfToxicsSec = scenarioEndingInError ? 5 * 50 : 2;
        final String topicName = randomName();
        createTopic(topicName, true);
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
        SupplierEx<PulsarClient> connectionSupplier = () -> PulsarClient.builder()
                                                                        .serviceUrl(toxicUrl)
                                                                        .connectionTimeout(2, SECONDS)
                                                                        .operationTimeout(50, SECONDS)
                                                                        .enableTransaction(true).build();
        pipeline.readFrom(Sources.mapJournal(inputMap, JournalInitialPosition.START_FROM_OLDEST))
                .withIngestionTimestamps()
                .map(Map.Entry::getValue)
                .writeTo(PulsarSinks.builder(() -> Schema.DOUBLE, Integer::doubleValue)
                                    .topic(topicName)
                                    .httpServiceUrl(pulsarContainer.getHttpServiceUrl())
                                    .connectionSupplier(connectionSupplier)
                                    .transactionTimeout(Duration.ofMinutes(1))
                                    .producerCustomizer(pb -> pb.sendTimeout(1, MINUTES))
                                    .build());

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(processingGuarantee);
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
            assertTrueEventually(() -> assertThat(job).eventuallyHasStatus(JobStatus.RUNNING));
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
                assertTrueEventually(() -> assertThat(resultList).hasSize(size), 120);
            }
        }
    }

    @NightlyTest
    @Timeout(value = 25, unit = MINUTES)
    @ParameterizedTest(name = "{index} - processingGuarantee={0}")
    @EnumSource(ProcessingGuarantee.class)
    public void sendAsyncQueueBlocksNotThrows(ProcessingGuarantee processingGuarantee) {
        final String topicName = randomName();
        createTopic(topicName, false);

        var pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(1000))
                .withIngestionTimestamps()
                .setLocalParallelism(1)
                .map(SimpleEvent::timestamp)
                .writeTo(PulsarSinks.builder(() -> Schema.INT64, FunctionEx.identity())
                                    .topic(topicName)
                                    .httpServiceUrl(pulsarContainer.getHttpServiceUrl())
                                    .connectionSupplier(getConnectionSupplier())
                                    .producerCustomizer(pb -> pb.maxPendingMessages(1))
                                    .build())
                .setLocalParallelism(1);

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(processingGuarantee);

        Predicate<LogEvent> logEventPredicate = event ->
            event.getThrown() != null && event.getThrown().getMessage() != null
                && event.getThrown().getMessage().contains("Producer send queue is full");
        try (LogEntryMatcher matcher = LogEntryMatcher.install(logEventPredicate)) {
            job = instances[0].getJet().newJob(pipeline, config);
            assertTrueEventually(() -> assertThat(job).eventuallyHasStatus(JobStatus.RUNNING));
            sleepSeconds(10);
            assertFalse(matcher.matched());
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

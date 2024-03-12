/*
 * Copyright 2024 Hazelcast Inc.
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
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.kafka.connect.KafkaConnectSources.connect;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.connect.data.Values.convertToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class KafkaConnectIT extends SimpleTestInClusterSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    public static final int ITEM_COUNT = 1_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectIT.class);

    @Test
    public void test_non_serializable() {
        assertThatThrownBy(() -> Pipeline.create()
                                     .readFrom(connect(new Properties(), new NonSerializableMapping()))
                                     .withIngestionTimestamps()
                                     .writeTo(Sinks.logger()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("\"projectionFn\" must be serializable");
    }
    private static class NonSerializableMapping implements FunctionEx<SourceRecord, Object> {
        @SuppressWarnings("unused")
        private final Object nonSerializableField = new Object();

        @Override
        public Object applyEx(SourceRecord sourceRecord) {
            return sourceRecord;
        }
    }

    @Test
    public void test_reading_without_timestamps() throws URISyntaxException {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "orders");
        randomProperties.setProperty("quickstart", "orders");

        Pipeline pipeline = Pipeline.create();
        StreamStage<SourceRecord> streamStage = pipeline.readFrom(connect(randomProperties))
                .withoutTimestamps()
                .setLocalParallelism(1);
        streamStage
                .writeTo(assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(config);
        Job job = hz.getJet().newJob(pipeline, jobConfig);

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
    public void windowing_withNativeTimestamps_should_fail_when_records_without_native_timestamps()
            throws URISyntaxException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "orders");
        randomProperties.setProperty("quickstart", "orders");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(connect(randomProperties, ConnectRecord::hashCode))
                .withNativeTimestamps(0)
                .setLocalParallelism(1)
                .window(WindowDefinition.tumbling(5))
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.noop());

        assertThatThrownBy(() -> hazelcastInstance.getJet().newJob(pipeline, jobConfig).join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("Neither timestampFn nor nativeEventTime specified");
    }

    @Test
    public void windowing_withIngestionTimestamps_should_work() throws URISyntaxException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);

        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "orders");
        randomProperties.setProperty("quickstart", "orders");

        Pipeline pipeline = Pipeline.create();
        StreamStage<WindowResult<Long>> streamStage = pipeline.readFrom(connect(randomProperties, ConnectRecord::hashCode))
                .withIngestionTimestamps()
                .setLocalParallelism(1)
                .window(WindowDefinition.tumbling(5))
                .aggregate(AggregateOperations.counting());
        streamStage
                .writeTo(assertCollectedEventually(60,
                        list -> assertThat(list).hasSizeGreaterThan(ITEM_COUNT)));

        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);

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
    public void test_reading_and_writing_to_map() throws URISyntaxException {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "orders");
        randomProperties.setProperty("quickstart", "orders");

        Pipeline pipeline = Pipeline.create();
        StreamStage<Map.Entry<String, Order>> streamStage = pipeline
                .readFrom(connect(randomProperties,
                        rec -> entry(convertToString(rec.keySchema(), rec.key()), new Order(rec))))
                .withoutTimestamps()
                .setLocalParallelism(1);
        streamStage
                .writeTo(Sinks.map(randomMapName()));
        streamStage
                .writeTo(assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(config);
        Job job = hz.getJet().newJob(pipeline, jobConfig);
        MetricsRegistry metricsRegistry = getNode(hz).nodeEngine.getMetricsRegistry();

        var collector = new KafkaMetricsCollector();
        ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
        try {
            es.scheduleWithFixedDelay(() -> metricsRegistry.collect(collector), 10, 10, MILLISECONDS);
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
            assertTrueEventually(() -> assertThat(collector.getSourceRecordPollTotal()).isGreaterThan(ITEM_COUNT));
        } finally {
            shutdownAndAwaitTermination(es);
        }
    }

    @Test
    public void test_scaling() throws URISyntaxException {
        final int instanceCount = 3;
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
        StreamStage<Order> streamStage = pipeline
                .readFrom(connect(randomProperties, Order::new))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism);
        streamStage
                .writeTo(assertCollectedEventually(120, list -> assertThat(list).hasSize(ITEM_COUNT * tasksMax)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(config, instanceCount);

        HazelcastInstance hazelcastInstance = hazelcastInstances[0];
        Job job = hazelcastInstance.getJet().newJob(pipeline, jobConfig);
        var collectors = new MultiNodeMetricsCollector<>(hazelcastInstances, new KafkaMetricsCollector());
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));

            assertTrueEventually(() -> {
                assertThat(collectors.collector().getSourceRecordPollTotal()).isGreaterThan(ITEM_COUNT);
            });
        } finally {
            collectors.close();
        }
    }

    private static String getTaskId(Order order) {
        return order.headers.get("task.id");
    }

    @Nonnull
    private static Map<String, List<Order>> groupByTaskId(List<Order> list) {
        return list.stream()
                .collect(Collectors.groupingBy(KafkaConnectIT::getTaskId,
                        Collectors.mapping(Function.identity(), toList())));
    }

    @Test
    public void test_snapshotting() throws URISyntaxException {
        Config config = smallInstanceConfig();
        enableEventJournal(config);
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        int localParallelism = 3;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("tasks.max", "3");
        randomProperties.setProperty("kafka.topic", "not-used");
        randomProperties.setProperty("quickstart", "orders");

        Pipeline pipeline = Pipeline.create();
        StreamStage<Order> streamStage = pipeline
                .readFrom(connect(randomProperties, Order::new))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism);
        streamStage.writeTo(Sinks.list("testResults"));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());
        enableSnapshotting(jobConfig);

        Job job = hazelcastInstance.getJet().newJob(pipeline, jobConfig);

        List<Order> testResults = hazelcastInstance.getList("testResults");

        Map<String, Integer> minOrderIdByTaskIdBeforeSuspend = new HashMap<>();
        assertTrueEventually(() -> {
            Map<String, List<Order>> ordersByTaskId = groupByTaskId(testResults);
            assertThat(ordersByTaskId.keySet()).hasSize(localParallelism);
            assertThat(ordersByTaskId).allSatisfy((taskId, records) ->
                    assertThat(records.size()).isGreaterThan(ITEM_COUNT)
            );
            minOrderIdByTaskIdBeforeSuspend.putAll(getMinOrderIdByTaskId(ordersByTaskId));
            LOGGER.debug("Min order ids before snapshot = {}", minOrderIdByTaskIdBeforeSuspend);
            LOGGER.debug("Max order ids before snapshot = {}", getMaxOrderIdByTaskId(ordersByTaskId));
        });

        waitForNextSnapshot(hazelcastInstance, job);
        assertJobStatusEventually(job, RUNNING);
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);

        testResults.clear();
        job.resume();

        Map<String, Integer> minOrderIdByTaskIdAfterSuspend = new HashMap<>();
        assertTrueEventually(() -> {
            Map<String, List<Order>> ordersByTaskId = groupByTaskId(testResults);

            assertThat(ordersByTaskId.keySet()).hasSize(localParallelism);
            assertThat(ordersByTaskId).allSatisfy((taskId, records) ->
                    assertThat(records.size()).isGreaterThan(ITEM_COUNT)
            );
            minOrderIdByTaskIdAfterSuspend.putAll(getMinOrderIdByTaskId(ordersByTaskId));
            LOGGER.debug("Min order ids after snapshot = {}", minOrderIdByTaskIdAfterSuspend);
            LOGGER.debug("Max order ids after snapshot = {}", getMaxOrderIdByTaskId(ordersByTaskId));
            job.cancel();
        });
        assertJobStatusEventually(job, FAILED);
        for (Map.Entry<String, Integer> minOrderId : minOrderIdByTaskIdAfterSuspend.entrySet()) {
            String taskId = minOrderId.getKey();
            assertThat(minOrderId.getValue()).isGreaterThan(minOrderIdByTaskIdBeforeSuspend.get(taskId));
        }
    }

    private void shutdownAndAwaitTermination(ExecutorService executorService) {
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                // Cancel currently executing tasks
                executorService.shutdownNow();
            }
        } catch (InterruptedException ignored) {
        }
    }

    private URL getDataGenConnectorURL() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        final String CONNECTOR_FILE_PATH = "confluentinc-kafka-connect-datagen-0.6.0.zip";
        URL resource = classLoader.getResource(CONNECTOR_FILE_PATH);
        assert resource != null;
        assertThat(new File(resource.toURI())).exists();
        return resource;
    }

    private void waitForNextSnapshot(HazelcastInstance hazelcastInstance, Job job) {
        JobRepository jobRepository = new JobRepository(hazelcastInstance);
        waitForNextSnapshot(jobRepository, job.getId(), 30, false);
    }

    private static void enableSnapshotting(JobConfig jobConfig) {
        jobConfig.setProcessingGuarantee(AT_LEAST_ONCE);
        jobConfig.setSnapshotIntervalMillis(10);
    }

    private static void enableEventJournal(Config config) {
        config.addMapConfig(new MapConfig("*")
                .setEventJournalConfig(new EventJournalConfig().setEnabled(true))
                .setBackupCount(3)
        );
    }

    private static Map<String, Integer> getMinOrderIdByTaskId(Map<String, List<Order>> ordersByTaskId) {
        return ordersByTaskId.entrySet().stream()
                .map(e -> entry(e.getKey(), e.getValue().stream().min(Comparator.comparingInt(o -> o.orderId)).get()))
                .collect(toMap(Map.Entry::getKey, e -> e.getValue().orderId));
    }

    private static Map<String, Integer> getMaxOrderIdByTaskId(Map<String, List<Order>> ordersByTaskId) {
        return ordersByTaskId.entrySet().stream()
                .map(e -> entry(e.getKey(), e.getValue().stream().max(Comparator.comparingInt(o -> o.orderId)).get()))
                .collect(toMap(Map.Entry::getKey, e -> e.getValue().orderId));
    }

    static class Order implements Serializable {
        final Integer orderId;
        final long orderTime;
        final String itemId;
        final double orderUnits;
        final Map<String, String> headers = new HashMap<>();

        Order(SourceRecord rec) {
            Struct struct = Values.convertToStruct(rec.valueSchema(), rec.value());
            orderId = struct.getInt32("orderid");
            orderTime = struct.getInt64("ordertime");
            itemId = struct.getString("itemid");
            orderUnits = struct.getFloat64("orderunits");
            for (Header header : rec.headers()) {
                headers.put(header.key(), header.value().toString());
            }
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderId=" + orderId +
                    ", orderTime=" + orderTime +
                    ", itemtId='" + itemId + '\'' +
                    ", orderUnits=" + orderUnits +
                    ", headers=" + headers +
                    '}';
        }
    }

    private static class KafkaMetricsCollector implements MetricsCollector {
        private final AtomicLong sourceRecordPollTotal = new AtomicLong();
        private final AtomicLong sourceRecordPollAvgTime = new AtomicLong();

        public Long getSourceRecordPollTotal() {
            return sourceRecordPollTotal.get();
        }

        public Long getSourceRecordPollAvgTime() {
            return sourceRecordPollAvgTime.get();
        }

        @Override
        public void collectLong(MetricDescriptor descriptor, long value) {
            String name = descriptor.toString();
            if (name.contains("sourceRecordPollTotalAvgTime")) {
                sourceRecordPollAvgTime.addAndGet(value);
            } else if (name.contains("sourceRecordPollTotal")) {
                sourceRecordPollTotal.addAndGet(value);
            }
        }

        @Override
        public void collectDouble(MetricDescriptor descriptor, double value) {
        }

        @Override
        public void collectException(MetricDescriptor descriptor, Exception e) {
        }

        @Override
        public void collectNoValue(MetricDescriptor descriptor) {
        }
    }
}

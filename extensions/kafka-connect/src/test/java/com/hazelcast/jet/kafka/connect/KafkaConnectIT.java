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
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.kafka.connect.KafkaConnectSources.connect;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.connect.data.Values.convertToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({QuickTest.class})
public class KafkaConnectIT extends SimpleTestInClusterSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    public static final int ITEM_COUNT = 1_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectIT.class);
    private static final Properties RANDOM_PROPERTIES = new Properties();
    private static final Config CONFIG = smallInstanceConfig();

    static {
        RANDOM_PROPERTIES.setProperty("name", "datagen-connector");
        RANDOM_PROPERTIES.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        RANDOM_PROPERTIES.setProperty("max.interval", "1");
        RANDOM_PROPERTIES.setProperty("kafka.topic", "orders");
        RANDOM_PROPERTIES.setProperty("quickstart", "orders");

        CONFIG.getJetConfig().setResourceUploadEnabled(true);
        enableEventJournal(CONFIG);
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(1, CONFIG);
    }

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
        Pipeline pipeline = Pipeline.create();
        StreamStage<SourceRecord> streamStage = pipeline.readFrom(connect(RANDOM_PROPERTIES))
                .withoutTimestamps()
                .setLocalParallelism(1);
        streamStage
                .writeTo(assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Job job = instance().getJet().newJob(pipeline, jobConfig);

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

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(connect(RANDOM_PROPERTIES, ConnectRecord::hashCode))
                .withNativeTimestamps(0)
                .setLocalParallelism(1)
                .window(WindowDefinition.tumbling(5))
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.noop());

        assertThatThrownBy(() -> instance().getJet().newJob(pipeline, jobConfig).join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("Neither timestampFn nor nativeEventTime specified");
    }

    @Test
    public void windowing_withIngestionTimestamps_should_work() throws URISyntaxException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Pipeline pipeline = Pipeline.create();
        StreamStage<WindowResult<Long>> streamStage = pipeline.readFrom(connect(RANDOM_PROPERTIES, ConnectRecord::hashCode))
                .withIngestionTimestamps()
                .setLocalParallelism(1)
                .window(WindowDefinition.tumbling(5))
                .aggregate(AggregateOperations.counting());
        streamStage
                .writeTo(assertCollectedEventually(60,
                        list -> assertThat(list).hasSizeGreaterThan(ITEM_COUNT)));

        Job job = instance().getJet().newJob(pipeline, jobConfig);

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
        Pipeline pipeline = Pipeline.create();
        StreamStage<Map.Entry<String, Order>> streamStage = pipeline
                .readFrom(connect(RANDOM_PROPERTIES,
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

        // Create the collector before the job to make sure it is scheduled
        try (var collectors = new MultiNodeMetricsCollector<>(instances(), new KafkaMetricsCollector())) {
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

    private static String getTaskId(Order order) {
        return order.headers.get("task.id");
    }

    @Test
    public void test_snapshotting() throws URISyntaxException {
        int localParallelism = 3;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("tasks.max", "3");
        randomProperties.setProperty("max.interval", "1000");
        randomProperties.setProperty("kafka.topic", "not-used");
        randomProperties.setProperty("quickstart", "orders");

        // Sink for Map<String,List<Order>>
        String testResultsMap = "testResults" + System.currentTimeMillis();
        Sink<Order> sink = Sinks.<Order, String, List<Order>>mapBuilder(testResultsMap)
                                .toKeyFn(KafkaConnectIT::getTaskId)
                                .toValueFn(List::of)
                                .mergeFn((a, b) -> {
                                    ArrayList<Order> orders = new ArrayList<>(a);
                                    orders.addAll(b);
                                    return orders;
                                })
                                .build();

        Pipeline pipeline = Pipeline.create();
        StreamStage<Order> streamStage = pipeline
                .readFrom(connect(randomProperties, Order::new))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism)
                .peek(o -> "Order from taskId=" + getTaskId(o) + ", order=" + o);
        streamStage.writeTo(sink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());
        enableSnapshotting(jobConfig);

        Job job = instance().getJet().newJob(pipeline, jobConfig);

        Map<String, List<Order>> ordersByTaskId = instance().getMap(testResultsMap);

        Map<String, Integer> minOrderIdByTaskIdBeforeSuspend = new HashMap<>();
        assertTrueEventually(() -> {
            // Assert that each Kafka Connect Task has run
            assertThat(ordersByTaskId.keySet()).hasSize(localParallelism);
            // Assert that we have some Orders from each Kafka Connect Task
            assertThat(ordersByTaskId).allSatisfy((taskId, records) ->
                    assertThat(records).isNotEmpty()
            );
            minOrderIdByTaskIdBeforeSuspend.putAll(getMinOrderIdByTaskId(ordersByTaskId));
            LOGGER.debug("Min order ids before suspend = {}", minOrderIdByTaskIdBeforeSuspend);
            LOGGER.debug("Max order ids before suspend = {}", getMaxOrderIdByTaskId(ordersByTaskId));
        });

        waitForNextSnapshot(instance(), job);
        assertThat(job).eventuallyHasStatus(RUNNING);
        job.suspend();
        assertThat(job).eventuallyHasStatus(SUSPENDED);

        ordersByTaskId.clear();
        job.resume();

        Map<String, Integer> minOrderIdByTaskIdAfterSuspend = new HashMap<>();
        assertTrueEventually(() -> {
            // Assert that each Kafka Connect Task has run
            assertThat(ordersByTaskId.keySet()).hasSize(localParallelism);
            // Assert that we have some Orders from each Kafka Connect Task
            assertThat(ordersByTaskId).allSatisfy((taskId, records) ->
                    assertThat(records).isNotEmpty()
            );
            minOrderIdByTaskIdAfterSuspend.putAll(getMinOrderIdByTaskId(ordersByTaskId));
            LOGGER.debug("Min order ids after snapshot = {}", minOrderIdByTaskIdAfterSuspend);
            LOGGER.debug("Max order ids after snapshot = {}", getMaxOrderIdByTaskId(ordersByTaskId));
            job.cancel();
        });
        assertThat(job).eventuallyHasStatus(FAILED);
        for (Map.Entry<String, Integer> minOrderId : minOrderIdByTaskIdAfterSuspend.entrySet()) {
            String taskId = minOrderId.getKey();
            assertThat(minOrderId.getValue()).isGreaterThan(minOrderIdByTaskIdBeforeSuspend.get(taskId));
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

    public static void waitForNextSnapshot(HazelcastInstance hazelcastInstance, Job job) {
        JobRepository jobRepository = new JobRepository(hazelcastInstance);
        waitForNextSnapshot(jobRepository, job.getId(), 30, false);
    }

    private static void enableSnapshotting(JobConfig jobConfig) {
        jobConfig.setProcessingGuarantee(AT_LEAST_ONCE);
        jobConfig.setSnapshotIntervalMillis(200);
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

    static class KafkaMetricsCollector implements MetricsCollector {
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

/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.io.File;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.kafka.connect.KafkaConnectSources.connect;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.connect.data.Values.convertToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KafkaConnectIntegrationTest extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    public static final int ITEM_COUNT = 1_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectIntegrationTest.class);

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
        streamStage.writeTo(Sinks.logger());
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
            assertTrueEventually(() -> {
                List<Long> pollTotalList = getSourceRecordPollTotalList();
                assertThat(pollTotalList).isNotEmpty();
                Long sourceRecordPollTotal = pollTotalList.get(0);
                assertThat(sourceRecordPollTotal).isGreaterThan(ITEM_COUNT);
            });
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
        streamStage.writeTo(Sinks.logger());
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
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
        StreamStage<Map.Entry<String, Order>> streamStage = pipeline.readFrom(connect(randomProperties,
                        rec -> entry(convertToString(rec.keySchema(), rec.key()), new Order(rec))))
                .withoutTimestamps()
                .setLocalParallelism(1);
        streamStage
                .writeTo(Sinks.logger());
        streamStage
                .writeTo(Sinks.map(randomMapName()));
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
            assertTrueEventually(() -> {
                List<Long> pollTotalList = getSourceRecordPollTotalList();
                assertThat(pollTotalList).isNotEmpty();
                Long sourceRecordPollTotal = pollTotalList.get(0);
                assertThat(sourceRecordPollTotal).isGreaterThan(ITEM_COUNT);
            });
        }
    }

    private static <T> List<T> getMBeanValues(ObjectName objectName, String attribute) {
        return (List<T>) getMBeans(objectName).stream().map(i -> getAttribute(i, attribute)).collect(toList());
    }

    @Nonnull
    private static <T> T getAttribute(ObjectInstance objectInstance, String attribute) {
        try {
            return (T) ManagementFactory.getPlatformMBeanServer().getAttribute(objectInstance.getObjectName(), attribute);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    private static List<ObjectInstance> getMBeans(ObjectName objectName) {
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        return new ArrayList<>(platformMBeanServer.queryMBeans(objectName, null));
    }

    @Test
    @Ignore
    public void test_scaling() throws URISyntaxException {
        int localParallelism = 3;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "orders");
        randomProperties.setProperty("quickstart", "orders");

        Pipeline pipeline = Pipeline.create();
        StreamStage<Order> streamStage = pipeline.readFrom(connect(randomProperties, Order::new))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism);
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(120,
                        list -> {
                            Map<String, List<Order>> ordersByTaskId = groupByTaskId(list);
                            LOGGER.info("ordersByTaskId = " + countOrdersByTaskId(ordersByTaskId));
                            assertThat(ordersByTaskId).allSatisfy((taskId, records) ->
                                    assertThat(records.size()).isGreaterThan(ITEM_COUNT)
                            );
                        }));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hazelcastInstance = createHazelcastInstances(config, 3)[0];
        Job job = hazelcastInstance.getJet().newJob(pipeline, jobConfig);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
            assertTrueEventually(() -> {
                List<Long> sourceRecordPollTotalList = getSourceRecordPollTotalList();
                assertThat(sourceRecordPollTotalList).hasSize(localParallelism);
                assertThat(sourceRecordPollTotalList).allSatisfy(a -> assertThat(a).isGreaterThan(ITEM_COUNT));
            });
            assertTrueEventually(() -> {
                List<Long> times = getSourceRecordPollTotalTimes();
                assertThat(times).hasSize(localParallelism);
                assertThat(times).allSatisfy(a -> assertThat(a).isNotNegative());
            });
        }
    }

    @NotNull
    private static List<Map.Entry<String, Integer>> countOrdersByTaskId(Map<String, List<Order>> ordersByTaskId) {
        return ordersByTaskId.entrySet().stream().map(e -> entry(e.getKey(), e.getValue().size())).collect(toList());
    }

    private static String getTaskId(Order order) {
        return order.headers.get("task.id");
    }

    @Nonnull
    private static Map<String, List<Order>> groupByTaskId(List<Order> list) {
        return list.stream()
                .collect(Collectors.groupingBy(KafkaConnectIntegrationTest::getTaskId,
                        Collectors.mapping(Function.identity(), toList())));
    }

    private static List<Long> getSourceRecordPollTotalList() throws Exception {
        ObjectName objectName = new ObjectName("com.hazelcast:type=Metrics,prefix=kafka.connect,*");
        return getMBeanValues(objectName, "sourceRecordPollTotal");
    }

    private static List<Long> getSourceRecordPollTotalTimes() throws Exception {
        ObjectName objectName = new ObjectName("com.hazelcast:type=Metrics,prefix=kafka.connect,*");
        return getMBeanValues(objectName, "sourceRecordPollTotalAvgTime");
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
        randomProperties.setProperty("kafka.topic", "not-used");
        randomProperties.setProperty("quickstart", "orders");

        Pipeline pipeline = Pipeline.create();
        StreamStage<Order> streamStage = pipeline.readFrom(connect(randomProperties, Order::new))
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

    private URL getDataGenConnectorURL() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        final String CONNECTOR_FILE_PATH = "confluentinc-kafka-connect-datagen-0.6.0.zip";
        URL resource = classLoader.getResource(CONNECTOR_FILE_PATH);
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
}

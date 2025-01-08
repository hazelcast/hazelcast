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

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.CapturingCollector;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingRealTimeLag;
import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.DummyTask.dummyRecord;
import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.ITEMS_SIZE;
import static com.hazelcast.test.annotation.QuickTest.QUICK_TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(QUICK_TEST)
@SuppressWarnings({"JUnitMixedFramework", "DataFlowIssue"})
class ReadKafkaConnectPTest extends HazelcastTestSupport {

    private ReadKafkaConnectP<Integer> readKafkaConnectP;
    private TestOutbox outbox;
    private TestProcessorContext context;
    private HazelcastInstance hazelcastInstance;

    @BeforeEach
    void setUp() {
        hazelcastInstance = createHazelcastInstance(smallInstanceConfig());
        outbox = new TestOutbox(new int[]{10}, 10);
        context = new TestProcessorContext();
        context.setHazelcastInstance(hazelcastInstance);
        SourceConnectorWrapper sourceConnectorWrapper = new SourceConnectorWrapper(minimalProperties(), 0, context);
        readKafkaConnectP = new ReadKafkaConnectP<>(noEventTime(), rec -> (Integer) rec.value());
        readKafkaConnectP.setSourceConnectorWrapper(sourceConnectorWrapper);
        readKafkaConnectP.setActive(true);
    }

    @AfterEach
    void cleanup() {
        if (readKafkaConnectP != null) {
            readKafkaConnectP.close();
        }
        hazelcastInstance.shutdown();
    }

    @Test
    void should_run_task() throws Exception {
        readKafkaConnectP.init(outbox, context);

        assertTrueEventually(() -> assertTrue(readKafkaConnectP.configurationReceived()));
        boolean complete = readKafkaConnectP.complete();
        assertFalse(complete);
        assertThat(new ArrayList<>(outbox.queue(0))).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void should_filter_items() throws Exception {
        SourceConnectorWrapper sourceConnectorWrapper = new SourceConnectorWrapper(minimalProperties(), 0, context);
        readKafkaConnectP = new ReadKafkaConnectP<>(noEventTime(), rec -> {
            Integer value = (Integer) rec.value();
            if (value % 2 == 0) {
                return null;
            } else {
                return value;
            }
        });
        readKafkaConnectP.setActive(true);
        readKafkaConnectP.setSourceConnectorWrapper(sourceConnectorWrapper);

        readKafkaConnectP.init(outbox, context);
        assertTrueEventually(() -> assertTrue(readKafkaConnectP.configurationReceived()));
        boolean complete = readKafkaConnectP.complete();

        assertFalse(complete);
        assertThat(new ArrayList<>(outbox.queue(0))).containsExactly(1, 3);
    }

    @ParameterizedTest(name = "isActive={0}")
    @ValueSource(booleans = {true, false})
    void should_produce_watermarks(boolean isActive) throws Exception {

        var sourceConnectorWrapper = new SourceConnectorWrapper(minimalProperties(), 0, context);
        var policy = EventTimePolicy.eventTimePolicy(o -> System.currentTimeMillis(), limitingRealTimeLag(0),
                1, 0, 1);
        readKafkaConnectP = new ReadKafkaConnectP<>(policy, rec -> (Integer) rec.value());
        readKafkaConnectP.setActive(isActive);
        readKafkaConnectP.setSourceConnectorWrapper(sourceConnectorWrapper);
        readKafkaConnectP.eventTimeMapper().restoreWatermark(0, 1337);

        readKafkaConnectP.init(outbox, context);
        assertTrueEventually(() -> assertTrue(readKafkaConnectP.configurationReceived()));
        boolean complete = readKafkaConnectP.complete();

        assertFalse(complete);

        final Predicate<Object> isWatermark = o -> (o instanceof Watermark);
        var actual = new ArrayList<>(outbox.queue(0));
        assertThat(actual)
                .filteredOn(isWatermark)
                .withFailMessage("Watermark should be always present")
                .isNotEmpty();
        assertThat(readKafkaConnectP.eventTimeMapper().getWatermark(0)).isNotNegative();
        if (isActive) {
            assertThat(actual).filteredOn(isWatermark.negate()).containsExactly(0, 1, 2, 3, 4);
        }
    }

    @Test
    void should_require_eventTimePolicy() {
        var wrapper = new SourceConnectorWrapper(minimalProperties(), 0, context);
        assertThatThrownBy(() -> new ReadKafkaConnectP<>(null,
                rec -> (Integer) rec.value())
                .setSourceConnectorWrapper(wrapper))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("eventTimePolicy is required");
    }

    @Test
    void should_require_projectionFn() {
        var wrapper = new SourceConnectorWrapper(minimalProperties(), 0, context);
        assertThatThrownBy(() -> new ReadKafkaConnectP<>(noEventTime(), null)
                .setSourceConnectorWrapper(wrapper))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectionFn is required");
    }

    @Test
    void should_register_metrics() throws Exception {
        readKafkaConnectP.init(outbox, context);
        assertTrueEventually(() -> assertTrue(readKafkaConnectP.configurationReceived()));
        MetricsRegistry metricsRegistry = Util.getNodeEngine(hazelcastInstance).getMetricsRegistry();
        CapturingCollector collector = new CapturingCollector();
        metricsRegistry.collect(collector);
        List<String> metricNames = collector.captures().keySet().stream()
                .filter(metric -> metric.discriminatorValue() != null && metric.discriminatorValue().contains("some-"))
                .map(MetricDescriptor::metric)
                .collect(Collectors.toList());
        assertThat(metricNames).contains("sourceRecordPollTotal", "sourceRecordPollTotalAvgTime", "creationTime");
    }

    @Test
    void should_not_emit_when_snapshotting_but_after() throws Exception {
        enableSnapshotting(context);
        readKafkaConnectP.init(outbox, context);
        assertTrueEventually(() -> assertTrue(readKafkaConnectP.configurationReceived()));
        readKafkaConnectP.saveToSnapshot();
        readKafkaConnectP.complete();

        assertThat(outbox.queue(0)).isEmpty();

        readKafkaConnectP.snapshotCommitFinish(true);

        readKafkaConnectP.complete();

        assertThat(new ArrayList<>(outbox.queue(0))).containsExactly(0, 1, 2, 3, 4);

    }

    @Test
    void should_close_task_runner() throws Exception {
        readKafkaConnectP.setSourceConnectorWrapper(null);
        readKafkaConnectP.setPropertiesFromUser(minimalProperties());
        readKafkaConnectP.init(outbox, context);
        assertTrueEventually(() -> assertTrue(readKafkaConnectP.configurationReceived()));

        readKafkaConnectP.complete();

        assertTrueEventually(() -> assertThat(DummySourceConnector.DummyTask.INSTANCE.isStarted()).isTrue());
        readKafkaConnectP.close();
        assertThat(DummySourceConnector.DummyTask.INSTANCE.isStopped()).isTrue();
        readKafkaConnectP = null;

    }

    @Test
    void should_create_snapshot() throws Exception {
        TestProcessorContext testProcessorContext = context;
        enableSnapshotting(testProcessorContext);
        testProcessorContext.setTotalParallelism(2);
        testProcessorContext.setGlobalProcessorIndex(1);
        readKafkaConnectP.init(outbox, testProcessorContext);
        assertTrueEventually(() -> assertTrue(readKafkaConnectP.configurationReceived()));

        readKafkaConnectP.complete();

        boolean snapshot = readKafkaConnectP.saveToSnapshot();
        assertTrue(snapshot);
        Map.Entry<Object, Object> lastSnapshot = outbox.snapshotQueue().peek();
        assertThat(lastSnapshot).isNotNull();
        assertThat(lastSnapshot.getKey()).isEqualTo(BroadcastKey.broadcastKey("snapshot-0"));
    }

    @Test
    void should_restore_snapshot() throws Exception {
        TestProcessorContext testProcessorContext = context;
        enableSnapshotting(testProcessorContext);
        readKafkaConnectP.init(outbox, testProcessorContext);
        assertTrueEventually(() -> assertTrue(readKafkaConnectP.configurationReceived()));
        Map.Entry<Object, Object> lastSnapshot = outbox.snapshotQueue().peek();
        assertThat(lastSnapshot).isNull();

        readKafkaConnectP.restoreFromSnapshot(BroadcastKey.broadcastKey("snapshot-0"), stateWithOffset());

        readKafkaConnectP.saveToSnapshot();
        lastSnapshot = outbox.snapshotQueue().peek();
        assertThat(lastSnapshot).isNotNull();
        assertThat((State) lastSnapshot.getValue()).isEqualTo(stateWithOffset());

    }

    @Nonnull
    private static State stateWithOffset() {
        Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();
        SourceRecord lastRecord = dummyRecord(42);
        partitionsToOffset.put(lastRecord.sourcePartition(), lastRecord.sourceOffset());
        return new State(partitionsToOffset);
    }

    private static void enableSnapshotting(TestProcessorContext testProcessorContext) {
        testProcessorContext.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
    }

    @Nonnull
    private static Properties minimalProperties() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("tasks.max", "2");
        properties.setProperty("connector.class", DummySourceConnector.class.getName());
        properties.setProperty(ITEMS_SIZE, "5");
        return properties;
    }
}

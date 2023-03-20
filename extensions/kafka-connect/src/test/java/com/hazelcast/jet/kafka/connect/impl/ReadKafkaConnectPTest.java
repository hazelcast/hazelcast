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

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.DummyTask.dummyRecord;
import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.ITEMS_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadKafkaConnectPTest {

    private ReadKafkaConnectP readKafkaConnectP;
    private TestOutbox outbox;

    @Before
    public void setUp() {
        ConnectorWrapper connectorWrapper = new ConnectorWrapper(minimalProperties());
        readKafkaConnectP = new ReadKafkaConnectP(connectorWrapper, noEventTime());
        outbox = new TestOutbox(new int[]{10}, 10);
    }

    @Test
    public void should_run_task() throws Exception {
        readKafkaConnectP.init(outbox, new TestProcessorContext());
        boolean complete = readKafkaConnectP.complete();

        assertFalse(complete);
        assertThat(new ArrayList<>(outbox.queue(0))).containsExactly(dummyRecord(0), dummyRecord(1), dummyRecord(2));
    }

    @Test
    public void should_not_emit_when_snapshotting_but_after() throws Exception {
        TestProcessorContext testProcessorContext = new TestProcessorContext();
        enableSnapshotting(testProcessorContext);
        readKafkaConnectP.init(outbox, testProcessorContext);
        readKafkaConnectP.saveToSnapshot();
        readKafkaConnectP.complete();

        assertThat(outbox.queue(0)).isEmpty();

        readKafkaConnectP.snapshotCommitFinish(true);

        readKafkaConnectP.complete();

        assertThat(new ArrayList<>(outbox.queue(0))).containsExactly(dummyRecord(0), dummyRecord(1), dummyRecord(2));

    }

    @Test
    public void should_close_task_runner() throws Exception {
        readKafkaConnectP.init(outbox, new TestProcessorContext());

        readKafkaConnectP.complete();

        assertThat(DummySourceConnector.DummyTask.INSTANCE.isStarted()).isTrue();
        readKafkaConnectP.close();
        assertThat(DummySourceConnector.DummyTask.INSTANCE.isStopped()).isTrue();

    }

    @Test
    public void should_create_snapshot() throws Exception {
        TestProcessorContext testProcessorContext = new TestProcessorContext();
        enableSnapshotting(testProcessorContext);
        testProcessorContext.setTotalParallelism(2);
        testProcessorContext.setGlobalProcessorIndex(1);
        readKafkaConnectP.init(outbox, testProcessorContext);

        readKafkaConnectP.complete();

        boolean snapshot = readKafkaConnectP.saveToSnapshot();
        assertTrue(snapshot);
        Map.Entry<Object, Object> lastSnapshot = outbox.snapshotQueue().peek();
        assertThat(lastSnapshot).isNotNull();
        assertThat(lastSnapshot.getKey()).isEqualTo(BroadcastKey.broadcastKey("snapshot-1"));
    }

    @Test
    public void should_restore_snapshot() throws Exception {
        TestProcessorContext testProcessorContext = new TestProcessorContext();
        enableSnapshotting(testProcessorContext);
        readKafkaConnectP.init(outbox, testProcessorContext);
        Map.Entry<Object, Object> lastSnapshot = outbox.snapshotQueue().peek();
        assertThat(lastSnapshot).isNull();

        readKafkaConnectP.restoreFromSnapshot(BroadcastKey.broadcastKey("snapshot-0"), stateWithOffset(42));

        readKafkaConnectP.saveToSnapshot();
        lastSnapshot = outbox.snapshotQueue().peek();
        assertThat(lastSnapshot).isNotNull();
        assertThat((TaskRunner.State) lastSnapshot.getValue()).isEqualTo(stateWithOffset(42));

    }

    @NotNull
    private static TaskRunner.State stateWithOffset(int value) {
        Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();
        SourceRecord lastRecord = dummyRecord(value);
        partitionsToOffset.put(lastRecord.sourcePartition(), lastRecord.sourceOffset());
        TaskRunner.State state = new TaskRunner.State(partitionsToOffset);
        return state;
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
        properties.setProperty(ITEMS_SIZE, "3");
        return properties;
    }
}

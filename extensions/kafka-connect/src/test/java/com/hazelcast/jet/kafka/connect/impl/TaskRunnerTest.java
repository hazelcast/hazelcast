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

import com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.DummyTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.DummyTask.dummyRecord;
import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.ITEMS_SIZE;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TaskRunnerTest {

    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    private static final int CONFIGURED_ITEMS_SIZE = 3;
    private final DummySourceConnector connector = new DummySourceConnector();
    private final TaskRunner taskRunner = new TaskRunner("some-task-name", new State(), DummyTask::new);


    @Before
    public void setUp() {
        connector.start(minimalProperties());
        taskRunner.updateTaskConfig(dummyTaskConfig());
    }

    @Test
    public void should_poll_data() {
        assertPolledRecordsSize(CONFIGURED_ITEMS_SIZE);
        assertThat(DummyTask.INSTANCE.isStarted()).isTrue();
        assertThat(DummyTask.INSTANCE.isInitialized()).isTrue();
    }

    @Test
    public void should_not_poll_data_without_task_config() {
        taskRunner.updateTaskConfig(null);
        assertPolledRecordsSize(0);
    }

    @Test
    public void should_commit_records() {
        for (SourceRecord sourceRecord : taskRunner.poll()) {
            taskRunner.commitRecord(sourceRecord);
        }

        DummyTask dummyTask = DummyTask.INSTANCE;
        assertThat(dummyTask.recordCommitted(dummyRecord(0))).isTrue();
        assertThat(dummyTask.recordCommitted(dummyRecord(1))).isTrue();
        assertThat(dummyTask.recordCommitted(dummyRecord(2))).isTrue();
    }

    @Test
    public void should_commit() {
        taskRunner.poll();

        taskRunner.commit();

        DummyTask dummyTask = DummyTask.INSTANCE;
        assertThat(dummyTask.wasCommit()).isTrue();
    }

    private Map<String, String> dummyTaskConfig() {
        return connector.taskConfigs(1).get(0);
    }

    @Test
    public void should_stop_and_recreate_task() {
        taskRunner.poll();
        DummyTask taskInstance_1 = DummyTask.INSTANCE;
        assertThat(taskInstance_1.isStarted()).isTrue();

        taskRunner.stop();
        assertThat(taskInstance_1.isStopped()).isTrue();
        connector.setProperty(ITEMS_SIZE, String.valueOf(5));
        taskRunner.updateTaskConfig(dummyTaskConfig());

        assertPolledRecordsSize(5);

        DummyTask taskInstance_2 = DummyTask.INSTANCE;
        assertThat(taskInstance_2.isStarted()).isTrue();
        assertThat(taskInstance_1).isNotSameAs(taskInstance_2);
    }

    @Test
    public void should_reconfigure_task() {
        assertPolledRecordsSize(CONFIGURED_ITEMS_SIZE);
        connector.setProperty(ITEMS_SIZE, String.valueOf(5));
        assertPolledRecordsSize(CONFIGURED_ITEMS_SIZE);

        taskRunner.updateTaskConfig(dummyTaskConfig());

        assertPolledRecordsSize(5);
    }

    @Test
    public void should_create_snapshot() {
        for (SourceRecord sourceRecord : taskRunner.poll()) {
            taskRunner.commitRecord(sourceRecord);
        }
        State snapshot = taskRunner.createSnapshot();

        Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();
        SourceRecord lastRecord = dummyRecord(2);
        partitionsToOffset.put(lastRecord.sourcePartition(), lastRecord.sourceOffset());
        assertThat(snapshot).isEqualTo(new State(partitionsToOffset));
    }

    @Test
    public void should_restore_snapshot() {
        State initialSnapshot = taskRunner.createSnapshot();
        assertThat(initialSnapshot).isEqualTo(new State(new HashMap<>()));

        Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();
        SourceRecord sourceRecord = dummyRecord(42);
        partitionsToOffset.put(sourceRecord.sourcePartition(), sourceRecord.sourceOffset());
        State stateToRestore = new State(partitionsToOffset);

        taskRunner.restoreSnapshot(stateToRestore);

        State snapshot = taskRunner.createSnapshot();
        assertThat(snapshot).isEqualTo(new State(partitionsToOffset));
    }

    private void assertPolledRecordsSize(int expected) {
        List<SourceRecord> secondSourceRecords = taskRunner.poll();
        assertThat(secondSourceRecords).hasSize(expected);
    }

    @Nonnull
    private static Map<String, String> minimalProperties() {
        Map<String, String> taskProperties = new HashMap<>();
        taskProperties.put(ITEMS_SIZE, String.valueOf(CONFIGURED_ITEMS_SIZE));
        return taskProperties;
    }

}

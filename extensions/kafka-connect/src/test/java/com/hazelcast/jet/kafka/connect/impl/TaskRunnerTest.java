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
import com.hazelcast.test.OverridePropertyRule;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.ITEMS_SIZE;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;

public class TaskRunnerTest {

    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    private final DummySourceConnector connector = new DummySourceConnector();
    private final HashMap<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();
    private final TaskRunner taskRunner = new TaskRunner(connector, partitionsToOffset);

    @Test
    public void should_poll_data() {
        connector.start(minimalProperties());

        assertPolledRecordsSize(3);
        assertThat(DummyTask.INSTANCE.isStarted()).isTrue();
        assertThat(DummyTask.INSTANCE.isInitialized()).isTrue();
    }

    @Test
    public void should_stop_and_recreate_task() {
        connector.start(minimalProperties());

        taskRunner.poll();
        DummyTask taskInstance_1 = DummyTask.INSTANCE;
        assertThat(taskInstance_1.isStarted()).isTrue();

        taskRunner.stop();
        assertThat(taskInstance_1.isStopped()).isTrue();
        connector.setProperty(ITEMS_SIZE, String.valueOf(5));

        assertPolledRecordsSize(5);

        DummyTask taskInstance_2 = DummyTask.INSTANCE;
        assertThat(taskInstance_2.isStarted()).isTrue();
        assertThat(taskInstance_1).isNotSameAs(taskInstance_2);
    }

    @Test
    public void should_reconfigure_task() {
        connector.start(minimalProperties());
        assertPolledRecordsSize(3);
        connector.setProperty(ITEMS_SIZE, String.valueOf(5));
        assertPolledRecordsSize(3);

        taskRunner.requestReconfiguration();

        assertPolledRecordsSize(5);
    }

    private void assertPolledRecordsSize(int expected) {
        List<SourceRecord> secondSourceRecords = taskRunner.poll();
        assertThat(secondSourceRecords).hasSize(expected);
    }

    @Nonnull
    private static Map<String, String> minimalProperties() {
        Map<String, String> taskProperties = new HashMap<>();
        taskProperties.put(ITEMS_SIZE, String.valueOf(3));
        return taskProperties;
    }

}

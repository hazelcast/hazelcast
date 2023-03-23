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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.INSTANCE;
import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.ITEMS_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConnectorWrapperTest {
    @Test
    public void should_create_and_start_source_with_minimal_properties() {
        new ConnectorWrapper(minimalProperties());

        assertThat(sourceConnectorInstance().isInitialized()).isTrue();
        assertThat(sourceConnectorInstance().isStarted()).isTrue();
    }

    @Test
    public void should_create_task_runners() {
        ConnectorWrapper connectorWrapper = new ConnectorWrapper(minimalProperties());

        TaskRunner taskRunner1 = connectorWrapper.createTaskRunner();
        assertThat(taskRunner1.getName()).isEqualTo("some-name-task-0");
        taskRunner1.poll();
        Map<String, String> expectedTaskProperties = new HashMap<>();
        expectedTaskProperties.put("name", "some-name");
        expectedTaskProperties.put("connector.class", DummySourceConnector.class.getName());
        expectedTaskProperties.put("task.id", "0");
        assertThat(lastTaskInstance().getProperties()).containsAllEntriesOf(expectedTaskProperties);

        TaskRunner taskRunner2 = connectorWrapper.createTaskRunner();
        taskRunner2.poll();
        assertThat(taskRunner2.getName()).isEqualTo("some-name-task-1");
        expectedTaskProperties = new HashMap<>();
        expectedTaskProperties.put("name", "some-name");
        expectedTaskProperties.put("connector.class", DummySourceConnector.class.getName());
        expectedTaskProperties.put("task.id", "1");
        assertThat(lastTaskInstance().getProperties()).containsAllEntriesOf(expectedTaskProperties);
    }

    @Test
    public void should_reconfigure_task_runners() {
        ConnectorWrapper connectorWrapper = new ConnectorWrapper(minimalProperties());

        TaskRunner taskRunner1 = connectorWrapper.createTaskRunner();
        assertThat(taskRunner1.getName()).isEqualTo("some-name-task-0");
        taskRunner1.poll();
        Map<String, String> expectedTaskProperties = new HashMap<>();
        expectedTaskProperties.put("name", "some-name");
        expectedTaskProperties.put("connector.class", DummySourceConnector.class.getName());
        expectedTaskProperties.put("task.id", "0");
        assertThat(lastTaskInstance().getProperties()).containsAllEntriesOf(expectedTaskProperties);

        sourceConnectorInstance().setProperty("updated-property", "some-value");
        sourceConnectorInstance().triggerReconfiguration();
        taskRunner1.poll();

        expectedTaskProperties = new HashMap<>();
        expectedTaskProperties.put("name", "some-name");
        expectedTaskProperties.put("connector.class", DummySourceConnector.class.getName());
        expectedTaskProperties.put("task.id", "0");
        expectedTaskProperties.put("updated-property", "some-value");
        assertThat(lastTaskInstance().getProperties()).containsAllEntriesOf(expectedTaskProperties);
    }

    private static DummySourceConnector.DummyTask lastTaskInstance() {
        return DummySourceConnector.DummyTask.INSTANCE;
    }

    private static DummySourceConnector sourceConnectorInstance() {
        return INSTANCE;
    }

    @Test
    public void should_fail_with_connector_class_not_found() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("connector.class", "com.example.non.existing.Connector");
        assertThatThrownBy(() -> new ConnectorWrapper(properties))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Connector class 'com.example.non.existing.Connector' not found. " +
                        "Did you add the connector jar to the job?");
    }

    @Test
    public void should_cleanup_on_destroy() {
        Properties properties = minimalProperties();
        properties.setProperty(ITEMS_SIZE, String.valueOf(3));
        ConnectorWrapper connectorWrapper = new ConnectorWrapper(properties);
        assertThat(sourceConnectorInstance().isStarted()).isTrue();

        connectorWrapper.stop();

        assertThat(sourceConnectorInstance().isStarted()).isFalse();
    }

    @Nonnull
    private static Properties minimalProperties() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("tasks.max", "2");
        properties.setProperty("connector.class", DummySourceConnector.class.getName());
        return properties;
    }
}

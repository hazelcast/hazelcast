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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.SourceBufferImpl;
import com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.DummyTask;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.INSTANCE;
import static com.hazelcast.jet.kafka.connect.impl.DummySourceConnector.ITEMS_SIZE;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({QuickTest.class, ParallelJVMTest.class})
public class KafkaConnectSourceTest {
    @Test
    public void should_create_and_start_source_with_minimal_properties() {
        new KafkaConnectSource(minimalProperties());

        assertThat(INSTANCE.isInitialized()).isTrue();
        assertThat(INSTANCE.isStarted()).isTrue();
    }

    @Test
    public void should_fail_with_connector_class_not_found() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("connector.class", "com.example.non.existing.Connector");
        assertThatThrownBy(() -> new KafkaConnectSource(properties))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Connector class 'com.example.non.existing.Connector' not found. " +
                        "Did you add the connector jar to the job?");
    }


    @Test
    public void should_cleanup_on_destroy() {
        Properties properties = minimalProperties();
        properties.setProperty(ITEMS_SIZE, String.valueOf(3));
        SourceBufferImpl.Timestamped<SourceRecord> buf = new SourceBufferImpl.Timestamped<>();
        KafkaConnectSource kafkaConnectSource = new KafkaConnectSource(properties);
        kafkaConnectSource.fillBuffer(buf);
        assertThat(INSTANCE.isStarted()).isTrue();
        assertThat(DummyTask.INSTANCE.isStarted()).isTrue();

        kafkaConnectSource.destroy();

        assertThat(INSTANCE.isStarted()).isFalse();
        assertThat(DummyTask.INSTANCE.isStarted()).isFalse();
    }

    @Test
    public void should_fill_buffer_with_data() {
        Properties properties = minimalProperties();
        properties.setProperty(ITEMS_SIZE, String.valueOf(3));
        SourceBufferImpl.Timestamped<SourceRecord> buf = new SourceBufferImpl.Timestamped<>();
        KafkaConnectSource kafkaConnectSource = new KafkaConnectSource(properties);

        kafkaConnectSource.fillBuffer(buf);

        List<SourceRecord> sourceRecords = getSourceRecords(buf);
        List<Integer> values = getValues(sourceRecords);
        assertThat(values).contains(0, 1, 2);
        assertThat(DummyTask.INSTANCE.isStarted()).isTrue();
        assertThat(DummyTask.INSTANCE.isInitialized()).isTrue();
    }

    @Test
    public void should_record_offset() {
        Properties properties = minimalProperties();
        properties.setProperty(ITEMS_SIZE, String.valueOf(3));
        SourceBufferImpl.Timestamped<SourceRecord> buf = new SourceBufferImpl.Timestamped<>();
        KafkaConnectSource kafkaConnectSource = new KafkaConnectSource(properties);
        assertThat(kafkaConnectSource.createSnapshot()).isEmpty();

        kafkaConnectSource.fillBuffer(buf);

        Map<Map<String, ?>, Map<String, ?>> snapshot = kafkaConnectSource.createSnapshot();
        assertThat(snapshot).containsEntry(mapOf("sourcePartition", 1), mapOf("sourceOffset", 2));
    }

    @Test
    public void should_restore_offset() {
        Properties properties = minimalProperties();
        properties.setProperty(ITEMS_SIZE, String.valueOf(3));
        SourceBufferImpl.Timestamped<SourceRecord> buf = new SourceBufferImpl.Timestamped<>();
        KafkaConnectSource kafkaConnectSource = new KafkaConnectSource(properties);
        kafkaConnectSource.fillBuffer(buf);
        Map<Map<String, ?>, Map<String, ?>> snapshotToRestore = kafkaConnectSource.createSnapshot();
        //create new source and restore the state
        Properties newProperties = minimalProperties();
        newProperties.setProperty(ITEMS_SIZE, String.valueOf(5));
        KafkaConnectSource otherKafkaConnectSource = new KafkaConnectSource(newProperties);

        otherKafkaConnectSource.restoreSnapshot(singletonList(snapshotToRestore));
        otherKafkaConnectSource.fillBuffer(buf);

        Map<Map<String, ?>, Map<String, ?>> otherSnapshot = otherKafkaConnectSource.createSnapshot();
        assertThat(otherSnapshot).containsEntry(mapOf("sourcePartition", 1), mapOf("sourceOffset", 6));

    }

    @Nonnull
    private static Map<String, Object> mapOf(String key, int value) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    @Nonnull
    private static List<Integer> getValues(List<SourceRecord> sourceRecords) {
        return sourceRecords.stream().map(s -> (int) s.value()).collect(Collectors.toList());
    }

    @Nonnull
    private static List<SourceRecord> getSourceRecords(SourceBufferImpl.Timestamped<SourceRecord> buf) {
        Traverser<JetEvent<SourceRecord>> traverser = buf.traverse();
        JetEvent<SourceRecord> event;
        List<SourceRecord> sourceRecords = new ArrayList<>();
        while ((event = traverser.next()) != null) {
            sourceRecords.add(event.payload());
        }
        return sourceRecords;
    }

    @Nonnull
    private static Properties minimalProperties() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("connector.class", DummySourceConnector.class.getName());
        return properties;
    }
}

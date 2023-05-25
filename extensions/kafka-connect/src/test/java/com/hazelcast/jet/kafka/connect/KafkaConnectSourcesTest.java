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

import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.jet.kafka.connect.KafkaConnectSources.connect;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KafkaConnectSourcesTest {

    @Test
    public void should_fail_when_no_name_property() {
        Properties properties = new Properties();
        assertThatThrownBy(() -> connect(properties, SourceRecordUtil::convertToString))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property 'name' is required");
    }

    @Test
    public void should_fail_when_no_projectionFn() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("connector.class", "some-name");
        assertThatThrownBy(() -> connect(properties, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectionFn is required");
    }


    @Test
    public void should_fail_when_tasks_max_property_set() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("connector.class", "some-name");
        properties.setProperty("tasks.max", "1");
        assertThatThrownBy(() -> connect(properties, SourceRecordUtil::convertToString))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property 'tasks.max' not allowed. Use setLocalParallelism(1) in the pipeline instead");
    }

    @Test
    public void should_fail_when_no_connector_class_property() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        assertThatThrownBy(() -> connect(properties, SourceRecordUtil::convertToString))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property 'connector.class' is required");
    }

    @Test
    public void should_create_source_with_minimal_properties() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("connector.class", "some-name");
        StreamSource<SourceRecord> source = connect(properties, rec -> rec);
        assertThat(source).isNotNull();
    }
}

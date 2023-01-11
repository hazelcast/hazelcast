/*
 * Copyright 2021 Hazelcast Inc.
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
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaConnectSourcesTest {

    @Test
    public void should_fail_when_no_name_property() {
        Properties properties = new Properties();
        Assertions.assertThatThrownBy(() -> KafkaConnectSources.connect(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property 'name' is required");
    }

    @Test
    public void should_fail_when_no_connector_class_property() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        Assertions.assertThatThrownBy(() -> KafkaConnectSources.connect(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property 'connector.class' is required");
    }

    @Test
    public void should_create_source_with_minimal_properties() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("connector.class", "some-name");
        StreamSource<SourceRecord> source = KafkaConnectSources.connect(properties);
        assertThat(source).isNotNull();
    }
}

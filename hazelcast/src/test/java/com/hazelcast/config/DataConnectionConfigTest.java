/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataConnectionConfigTest extends HazelcastTestSupport {
    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_equals_and_hashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(DataConnectionConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }

    @Test
    public void should_serialize_with_empty_properties() {
        DataConnectionConfig originalConfig = new DataConnectionConfig()
                .setName("some-name")
                .setType("some-type")
                .setShared(false);

        Data data = serializationService.toData(originalConfig);
        DataConnectionConfig deserializedCopy = serializationService.toObject(data);

        assertThat(deserializedCopy).isEqualTo(originalConfig);
    }

    @Test
    public void should_serialize_with_NON_empty_properties() {

        Properties properties = new Properties();
        properties.setProperty("prop1", "val1");
        properties.setProperty("prop2", "val2");
        DataConnectionConfig originalConfig = new DataConnectionConfig()
                .setName("some-name")
                .setType("some-type")
                .setProperties(properties);

        Data data = serializationService.toData(originalConfig);
        DataConnectionConfig deserializedCopy = serializationService.toObject(data);
        assertThat(deserializedCopy).isEqualTo(originalConfig);
    }

    @Test
    public void should_work_with_set_getProperty() {

        DataConnectionConfig config = new DataConnectionConfig()
                .setName("some-name")
                .setType("some-type")
                .setProperty("prop1", "val1")
                .setProperty("prop2", "val2");

        assertThat(config.getProperty("prop1")).isEqualTo("val1");
        assertThat(config.getProperty("prop2")).isEqualTo("val2");

        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("prop1", "val1");
        expectedProperties.setProperty("prop2", "val2");
        assertThat(config.getProperties()).isEqualTo(expectedProperties);
    }
}

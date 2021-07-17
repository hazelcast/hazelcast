/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.YamlClientConfigBuilderTest;
import com.hazelcast.internal.config.SchemaViolationConfigurationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.hazelcast.config.YamlConfigBuilderTest.buildConfig;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class YamlRootAdditionalPropertiesTest {

    @Rule
    public ExpectedException expExc = ExpectedException.none();

    @Test
    public void testMisIndentedMemberConfigProperty_failsValidation() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> buildConfig("hazelcast:\n"
                        + "  instance-name: 'my-instance'\n"
                        + "cluster-name: 'my-cluster'\n")
                );
        assertEquals("Mis-indented hazelcast configuration property found: [cluster-name]", actual.getMessage());
    }

    @Test
    public void testMisIndented_NonConfigProperty_passes() {
        buildConfig("hazelcast:\n"
                + "  instance-name: 'my-instance'\n"
                + "other-prop: ''\n");
    }

    @Test
    public void testMisIndented_ClientConfigProperty_failsValidation() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> YamlClientConfigBuilderTest.buildConfig("hazelcast-client:\n"
                        + "  instance-name: 'my-instance'\n"
                        + "client-labels: 'my-lbl'\n")
        );
        assertEquals("Mis-indented hazelcast configuration property found: [client-labels]", actual.getMessage());
    }

    @Test
    public void multipleMisIndented_configProps() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> YamlClientConfigBuilderTest.buildConfig("hazelcast-client: {}\n"
                        + "instance-name: 'my-instance'\n"
                        + "client-labels: 'my-lbl'\n")
        );
        assertEquals(new SchemaViolationConfigurationException("2 schema violations found", "#", "#", asList(
                new SchemaViolationConfigurationException("Mis-indented hazelcast configuration property found: [instance-name]",
                        "#", "#", emptyList()),
                new SchemaViolationConfigurationException("Mis-indented hazelcast configuration property found: [client-labels]",
                        "#", "#", emptyList())
        )), actual);
    }
}

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.config.YamlConfigBuilderTest.buildConfig;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class YamlRootAdditionalPropertiesTest {

    @Rule
    public ExpectedException expExc = ExpectedException.none();

    @Rule
    public OverridePropertyRule indentationCheckEnabled = clear("hazelcast.yaml.config.indentation.check.enabled");

    @Test
    public void testMisIndentedMemberConfigProperty_failsValidation() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> buildConfig("hazelcast:\n"
                        + "  instance-name: 'my-instance'\n"
                        + "cluster-name: 'my-cluster'\n")
        );
        assertEquals(format("Mis-indented hazelcast configuration property found: [cluster-name]%n"
                + "Note: you can disable this validation by passing the "
                + "-Dhazelcast.yaml.config.indentation.check.enabled=false system property"), actual.getMessage());
    }

    @Test
    public void misIndentedRootProperty_validationDisabled() {
        indentationCheckEnabled.setOrClearProperty("false");
        buildConfig("hazelcast:\n"
                + "  instance-name: 'my-instance'\n"
                + "cluster-name: 'my-cluster'\n");
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
        assertEquals(format("Mis-indented hazelcast configuration property found: [client-labels]%n"
                + "Note: you can disable this validation by passing the "
                + "-Dhazelcast.yaml.config.indentation.check.enabled=false system property"), actual.getMessage());
    }

    @Test
    public void multipleMisIndented_configProps() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> YamlClientConfigBuilderTest.buildConfig("hazelcast-client: {}\n"
                        + "instance-name: 'my-instance'\n"
                        + "client-labels: 'my-lbl'\n")
        );
        assertEquals(new SchemaViolationConfigurationException(format("2 schema violations found%n"
                + "Note: you can disable this validation by passing the "
                + "-Dhazelcast.yaml.config.indentation.check.enabled=false system property"), "#", "#", asList(
                new SchemaViolationConfigurationException("Mis-indented hazelcast configuration property found: [instance-name]",
                        "#", "#", emptyList()),
                new SchemaViolationConfigurationException("Mis-indented hazelcast configuration property found: [client-labels]",
                        "#", "#", emptyList())
        )), actual);
    }
}

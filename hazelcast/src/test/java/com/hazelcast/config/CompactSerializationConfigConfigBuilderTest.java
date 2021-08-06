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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.ExternalizableEmployeeDTO;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

import static com.hazelcast.config.ConfigRecognizerTest.HAZELCAST_CLIENT_END_TAG;
import static com.hazelcast.config.ConfigRecognizerTest.HAZELCAST_CLIENT_START_TAG;
import static com.hazelcast.config.ConfigRecognizerTest.HAZELCAST_END_TAG;
import static com.hazelcast.config.ConfigRecognizerTest.HAZELCAST_START_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that we are able to parse and use the declarative
 * configuration for the CompactSerializationConfig. Since this
 * feature is in BETA, there is no XSD definition. That's why,
 * to parse declarative configuration for it, we have to disable
 * schema validation. The tests should be run serially to not
 * interfere with the other tests, as the schema validation
 * is disabled through a system property.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactSerializationConfigConfigBuilderTest extends HazelcastTestSupport {

    @Rule
    public OverridePropertyRule disableSchemaValidationRule
            = OverridePropertyRule.set("hazelcast.config.schema.validation.enabled", "false");

    @Test
    public void testCompactSerializationEnabledWithXmlClientConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\" />\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig clientConfig = buildXmlClientConfig(xml);
        assertTrue(clientConfig.getSerializationConfig().getCompactSerializationConfig().isEnabled());
    }

    @Test
    public void testCompactSerializationEnabledWithXmlConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\" />\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        Config config = buildXmlConfig(xml);
        assertTrue(config.getSerializationConfig().getCompactSerializationConfig().isEnabled());
    }

    @Test
    public void testCompactSerializationEnabledWithYamlClientConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n";

        ClientConfig clientConfig = buildYamlClientConfig(yaml);
        assertTrue(clientConfig.getSerializationConfig().getCompactSerializationConfig().isEnabled());
    }

    @Test
    public void testCompactSerializationEnabledWithYamlConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n";

        Config config = buildYamlConfig(yaml);
        assertTrue(config.getSerializationConfig().getCompactSerializationConfig().isEnabled());
    }

    @Test
    public void testRegisterClassWithExplicitSerializerWithXmlClientConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class type-name=\"obj\" serializer=\"example.serialization.EmployeeDTOSerializer\">"
                + "                    example.serialization.EmployeeDTO\n"
                + "                </class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig clientConfig = buildXmlClientConfig(xml);
        verifyExplicitSerializerIsUsed(clientConfig.getSerializationConfig());
    }

    @Test
    public void testRegisterClassWithExplicitSerializerWithXmlConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class type-name=\"obj\" serializer=\"example.serialization.EmployeeDTOSerializer\">"
                + "                    example.serialization.EmployeeDTO\n"
                + "                </class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        Config config = buildXmlConfig(xml);
        verifyExplicitSerializerIsUsed(config.getSerializationConfig());
    }

    @Test
    public void testRegisterClassWithExplicitSerializerWithYamlClientConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  type-name: obj\n"
                + "                  serializer: example.serialization.EmployeeDTOSerializer\n";

        ClientConfig clientConfig = buildYamlClientConfig(yaml);
        verifyExplicitSerializerIsUsed(clientConfig.getSerializationConfig());
    }

    @Test
    public void testRegisterClassWithExplicitSerializerWithYamlConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  type-name: obj\n"
                + "                  serializer: example.serialization.EmployeeDTOSerializer\n";

        Config config = buildYamlConfig(yaml);
        verifyExplicitSerializerIsUsed(config.getSerializationConfig());
    }

    @Test
    public void testRegisterClassWithReflectiveSerializerWithXmlClientConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class>example.serialization.ExternalizableEmployeeDTO</class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig clientConfig = buildXmlClientConfig(xml);
        verifyReflectiveSerializerIsUsed(clientConfig.getSerializationConfig());
    }

    @Test
    public void testRegisterClassWithReflectiveSerializerWithXmlConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class>example.serialization.ExternalizableEmployeeDTO</class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        Config config = buildXmlConfig(xml);
        verifyReflectiveSerializerIsUsed(config.getSerializationConfig());
    }

    @Test
    public void testRegisterClassWithReflectiveSerializerWithYamlClientConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.ExternalizableEmployeeDTO\n";

        ClientConfig clientConfig = buildYamlClientConfig(yaml);
        verifyReflectiveSerializerIsUsed(clientConfig.getSerializationConfig());
    }

    @Test
    public void testRegisterClassWithReflectiveSerializerWithYamlConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.ExternalizableEmployeeDTO\n";

        Config config = buildYamlConfig(yaml);
        verifyReflectiveSerializerIsUsed(config.getSerializationConfig());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testRegisterClassWithJustTypeNameWithXmlClientConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class type-name=\"employee\">example.serialization.EmployeeDTO</class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildXmlClientConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testRegisterClassWithJustTypeNameWithXmlConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class type-name=\"employee\">example.serialization.EmployeeDTO</class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        buildXmlConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testRegisterClassWithJustTypeNameWithYamlClientConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  type-name: employee\n";

        buildYamlClientConfig(yaml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testRegisterClassWithJustTypeNameWithYamlConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  type-name: employee\n";

        buildYamlConfig(yaml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testRegisterClassWithJustSerializerNameWithXmlClientConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class serializer=\"example.serialization.EmployeeDTOSerializer\">\n"
                + "                    example.serialization.EmployeeDTO\n"
                + "                </class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildXmlClientConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testRegisterClassWithJustSerializerNameWithXmlConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class serializer=\"example.serialization.EmployeeDTOSerializer\">\n"
                + "                    example.serialization.EmployeeDTO\n"
                + "                </class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        buildXmlConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testRegisterClassWithJustSerializerNameWithYamlClientConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  serializer: example.serialization.EmployeeDTOSerializer\n";

        buildYamlClientConfig(yaml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testRegisterClassWithJustSerializerNameWithYamlConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  serializer: example.serialization.EmployeeDTOSerializer\n";

        buildYamlConfig(yaml);
    }

    private void verifyReflectiveSerializerIsUsed(SerializationConfig serializationConfig) {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(CompactTestUtil.createInMemorySchemaService())
                .setConfig(serializationConfig)
                .build();

        ExternalizableEmployeeDTO object = new ExternalizableEmployeeDTO();
        Data data = serializationService.toData(object);
        assertFalse(object.usedExternalizableSerialization());

        ExternalizableEmployeeDTO deserializedObject = serializationService.toObject(data);
        assertFalse(deserializedObject.usedExternalizableSerialization());
    }

    private void verifyExplicitSerializerIsUsed(SerializationConfig serializationConfig) {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(CompactTestUtil.createInMemorySchemaService())
                .setConfig(serializationConfig)
                .build();

        EmployeeDTO object = new EmployeeDTO(1, 1);
        Data data = serializationService.toData(object);

        EmployeeDTO deserializedObject = serializationService.toObject(data);
        assertEquals(object, deserializedObject);
    }


    private ClientConfig buildXmlClientConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        return configBuilder.build();
    }

    private Config buildXmlConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    private ClientConfig buildYamlClientConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlClientConfigBuilder configBuilder = new YamlClientConfigBuilder(bis);
        return configBuilder.build();
    }

    private Config buildYamlConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlConfigBuilder configBuilder = new YamlConfigBuilder(bis);
        return configBuilder.build();
    }
}

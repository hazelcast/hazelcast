/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_END_TAG;
import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_START_TAG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class XmlSchemaValidationTest {

    @Test
    public void testXmlDeniesDuplicateClusterNameConfig() {
        expectDuplicateElementError("cluster-name", () -> {
            String clusterName = "<cluster-name>foobar</cluster-name>";
            buildConfig(HAZELCAST_START_TAG + clusterName + clusterName + HAZELCAST_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateNetworkConfig() {
        expectDuplicateElementError("network", () -> {
            String networkConfig = """
                        <network>
                            <join>
                                <multicast enabled="false"/>
                                <tcp-ip enabled="true"/>
                            </join>
                        </network>
                    """;
            buildConfig(HAZELCAST_START_TAG + networkConfig + networkConfig + HAZELCAST_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateLicenseKeyConfig() {
        expectDuplicateElementError("license-key", () -> {
            String licenseConfig = "    <license-key>foo</license-key>";
            buildConfig(HAZELCAST_START_TAG + licenseConfig + licenseConfig + HAZELCAST_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicatePropertiesConfig() {
        expectDuplicateElementError("properties", () -> {
            String propertiesConfig = """
                        <properties>
                            <property name='foo'>fooval</property>
                        </properties>
                    """;
            buildConfig(HAZELCAST_START_TAG + propertiesConfig + propertiesConfig + HAZELCAST_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicatePartitionGroupConfig() {
        expectDuplicateElementError("partition-group", () -> {
            String partitionConfig = """
                       <partition-group>
                          <member-group>
                              <interface>foo</interface>
                          </member-group>
                       </partition-group>
                    """;
            buildConfig(HAZELCAST_START_TAG + partitionConfig + partitionConfig + HAZELCAST_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateListenersConfig() {
        expectDuplicateElementError("listeners", () -> {
            String listenersConfig = """
                       <listeners>
                            <listener>foo</listener>

                       </listeners>
                    """;
            buildConfig(HAZELCAST_START_TAG + listenersConfig + listenersConfig + HAZELCAST_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateSerializationConfig() {
        expectDuplicateElementError("serialization", () -> {
            String serializationConfig = """
                           <serialization>
                            <portable-version>0</portable-version>
                            <data-serializable-factories>
                                <data-serializable-factory factory-id="1">com.hazelcast.examples.DataSerializableFactory
                                </data-serializable-factory>
                            </data-serializable-factories>
                            <portable-factories>
                                <portable-factory factory-id="1">com.hazelcast.examples.PortableFactory</portable-factory>
                            </portable-factories>
                            <serializers>
                                <global-serializer>com.hazelcast.examples.GlobalSerializerFactory</global-serializer>
                                <serializer type-class="com.hazelcast.examples.DummyType"
                                    class-name="com.hazelcast.examples.SerializerFactory"/>
                            </serializers>
                            <check-class-def-errors>true</check-class-def-errors>
                        </serialization>
                    """;
            buildConfig(HAZELCAST_START_TAG + serializationConfig + serializationConfig + HAZELCAST_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateSecurityConfig() {
        expectDuplicateElementError("security", () -> {
            String securityConfig = "   <security/>\n";
            buildConfig(HAZELCAST_START_TAG + securityConfig + securityConfig + HAZELCAST_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateMemberAttributesConfig() {
        expectDuplicateElementError("member-attributes", () -> {
            String memberAttConfig = """
                        <member-attributes>
                            <attribute name="attribute">1234.5678</attribute>
                        </member-attributes>
                    """;
            buildConfig(HAZELCAST_START_TAG + memberAttConfig + memberAttConfig + HAZELCAST_END_TAG);
        });
    }

    private void expectDuplicateElementError(String elName, ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining(elName);
    }

    private static Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

}

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

package com.hazelcast.client.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.Properties;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvalidConfigurationClientTest {

    @Test
    public void testWhenXmlValid() {
        String xml = getDraftXml();
        buildConfig(xml);
    }

    @Test
    public void testWhenValid_SmartRoutingEnabled() {
        buildConfig("smart-routing-enabled", "false");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInValid_SmartRoutingEnabled() {
        buildConfig("smart-routing-enabled", "false1");
    }

    @Test
    public void testWhenValid_RedoOperationEnabled() {
        buildConfig("redo-operation-enabled", "true");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInValid_RedoOperationEnabled() {
        buildConfig("redo-operation-enabled", "tr1ue");
    }

    @Test
    public void testWhenValid_SocketInterceptorEnabled() {
        buildConfig("socket-interceptor-enabled", "true");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_SocketInterceptorEnabled() {
        buildConfig("socket-interceptor-enabled", "ttrue");
    }

    @Test
    public void testWhenValid_AwsEnabled() {
        buildConfig("aws-enabled", "true");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_AwsEnabled() {
        buildConfig("aws-enabled", "falsee");
    }

    @Test
    public void testWhenValid_InsideAwsEnabled() {
        buildConfig("inside-aws-enabled", "true");
    }

    @Test
    public void WhenValid_CredentialsClassName() {
        buildConfig("credentials-factory-class-name", "com.hazelcast.security.ICredentialsFactory");
        buildConfig("credentials-factory-class-name", " com.hazelcast.security.ICredentialsFactory");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void WhenInvalid_CredentialsClassName() {
        buildConfig("credentials-factory-class-name", "com hazelcast.security.ICredentialsFactory");
    }

    @Test
    public void WhenValid_ListenerClassName() {
        buildConfig("listener-class-name", "com.hazelcast.client.config.Listener");
        buildConfig("listener-class-name", " com.hazelcast.client.config.Listener");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void WhenInvalid_ListenerClassName() {
        buildConfig("listener-class-name", "com hazelcast.client.config.Listener");
    }

    @Test
    public void WhenValid_NativeByteOrder() {
        buildConfig("use-native-byte-order", "true");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void WhenInValid_NativeByteOrder() {
        buildConfig("use-native-byte-order", "truue");
    }

    @Test
    public void WhenValid_LoadBalancerType() {
        buildConfig("load-balancer-type", "round-robin");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void WhenInvalid_LoadBalancerType() {
        buildConfig("load-balancer-type", "roundrobin");
    }

    @Test
    public void WhenValid_EvictionPolicy() {
        buildConfig("eviction-policy", "LRU");
    }

    @Test
    public void WhenValid_NearCacheInMemoryFormat() {
        buildConfig("near-cache-in-memory-format", "OBJECT");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void WhenInvalid_NearCacheInMemoryFormat() {
        buildConfig("near-cache-in-memory-format", "binaryyy");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_NearCacheTTLSeconds() {
        buildConfig("near-cache-time-to-live-seconds", "-1");
    }

    @Test
    public void testWhenValid_NearCacheTTLSeconds() {
        buildConfig("near-cache-time-to-live-seconds", "100");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_NearCacheMaxIdleSeconds() {
        buildConfig("near-cache-max-idle-seconds", "-1");
    }

    @Test
    public void testWhenValid_NearCacheMaxIdleSeconds() {
        buildConfig("near-cache-max-idle-seconds", "100");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_NearCacheEvictionSize() {
        buildConfig("near-cache-eviction-size", "-100");
    }

    @Test
    public void testWhenValid_NearCacheEvictionSize() {
        buildConfig("near-cache-eviction-size", "100");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClassPathConfigWhenNullClassLoader() {
        new ClientClasspathXmlConfig(null, "my_config.xml", System.getProperties());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClassPathConfigWhenNullProperties() {
        new ClientClasspathXmlConfig(Thread.currentThread().getContextClassLoader(), "my_config.xml", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClassPathConfigWhenNullResource() {
        new ClientClasspathXmlConfig(Thread.currentThread().getContextClassLoader(), null, System.getProperties());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClassPathConfigWhenNonExistentConfig() {
        new ClientClasspathXmlConfig(Thread.currentThread().getContextClassLoader(), "non-existent-client-config.xml", System.getProperties());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void WhenDuplicateTagsAdded() {
        String xml = "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n"
                + "  <network>\n"
                + "    <cluster-members>\n"
                + "      <address>127.0.0.1</address>\n"
                + "    </cluster-members>\n"
                + "  </network>\n"
                + "  <network>\n"
                + "    <cluster-members>\n"
                + "      <address>127.0.0.1</address>\n"
                + "    </cluster-members>\n"
                + "  </network>\n"
                + "</hazelcast-client>\n";
        buildConfig(xml);
    }

    String getDraftXml() {
        return "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n"
                + "  <network>\n"
                + "    <cluster-members>\n"
                + "      <address>127.0.0.1</address>\n"
                + "    </cluster-members>\n"
                + "    <smart-routing>${smart-routing-enabled}</smart-routing>\n"
                + "    <redo-operation>${redo-operation-enabled}</redo-operation>\n"
                + "    <socket-interceptor enabled=\"${socket-interceptor-enabled}\">\n"
                + "      <class-name>com.hazelcast.examples.MySocketInterceptor</class-name>\n"
                + "    </socket-interceptor>\n"
                + "    <aws enabled=\"${aws-enabled}\" connection-timeout-seconds=\"${aws-timeout}\">\n"
                + "      <inside-aws>${inside-aws-enabled}</inside-aws>\n"
                + "      <access-key>TEST_ACCESS_KEY</access-key>\n"
                + "      <secret-key>TEST_SECRET_KEY</secret-key>\n"
                + "      <iam-role>TEST_IAM_ROLE</iam-role>\n"
                + "    </aws>\n"
                + "  </network>\n"
                + "  <security>\n"
                + "    <credentials-factory class-name='${credentials-factory-class-name}'/>\n"
                + "  </security>\n"
                + "  <listeners>\n"
                + "    <listener>${listener-class-name}</listener>\n"
                + "  </listeners>\n"
                + "  <serialization>\n"
                + "    <portable-version>3</portable-version>\n"
                + "    <use-native-byte-order>${use-native-byte-order}</use-native-byte-order>\n"
                + "    <byte-order>${byte-order}</byte-order>\n"
                + "    <enable-compression>${enable-compression}</enable-compression>\n"
                + "    <enable-shared-object>${enable-shared-object}</enable-shared-object>\n"
                + "    <allow-unsafe>${allow-unsafe}</allow-unsafe>\n"
                + "  </serialization>\n"
                + "  <load-balancer type=\"${load-balancer-type}\"/>\n"
                + "  <near-cache name=\"testNearCache\">\n"
                + "    <time-to-live-seconds>${near-cache-time-to-live-seconds}</time-to-live-seconds>\n"
                + "    <max-idle-seconds>${near-cache-max-idle-seconds}</max-idle-seconds>\n"
                + "    <invalidate-on-change>${near-cache-invalidate-on-change}</invalidate-on-change>\n"
                + "    <in-memory-format>${near-cache-in-memory-format}</in-memory-format>\n"
                + "    <eviction size=\"${near-cache-eviction-size}\""
                + "       max-size-policy=\"${near-cache-eviction-max-size-policy}\""
                + "       eviction-policy=\"${near-cache-eviction-policy}\"/>\n"
                + "  </near-cache>"
                + "</hazelcast-client>\n";
    }

    Properties getDraftProperties() {
        Properties properties = new Properties();
        properties.setProperty("smart-routing-enabled", "true");
        properties.setProperty("redo-operation-enabled", "true");
        properties.setProperty("socket-interceptor-enabled", "true");
        properties.setProperty("aws-enabled", "true");
        properties.setProperty("aws-timeout", "10");
        properties.setProperty("inside-aws-enabled", "true");
        properties.setProperty("credentials-factory-class-name", "com.hazelcast.security.impl.DefaultCredentialsFactory");
        properties.setProperty("listener-class-name", "com.hazelcast.examples.MembershipListener");
        properties.setProperty("use-native-byte-order", "true");
        properties.setProperty("byte-order", "BIG_ENDIAN");
        properties.setProperty("enable-compression", "true");
        properties.setProperty("enable-shared-object", "true");
        properties.setProperty("allow-unsafe", "true");
        properties.setProperty("load-balancer-type", "random");
        properties.setProperty("in-memory-format", "OBJECT");

        properties.setProperty("near-cache-time-to-live-seconds", "10000");
        properties.setProperty("near-cache-max-idle-seconds", "5000");
        properties.setProperty("near-cache-invalidate-on-change", "true");
        properties.setProperty("near-cache-in-memory-format", "BINARY");
        properties.setProperty("near-cache-cache-local-entries", "true");
        properties.setProperty("near-cache-eviction-size", "100");
        properties.setProperty("near-cache-eviction-max-size-policy", "ENTRY_COUNT");
        properties.setProperty("near-cache-eviction-policy", "LRU");

        return properties;
    }

    ClientConfig buildConfig(String xml) {
        return buildConfig(xml, getDraftProperties());
    }

    ClientConfig buildConfig(String propertyKey, String propertyValue) {
        String xml = getDraftXml();
        Properties properties = getDraftProperties();
        properties.setProperty(propertyKey, propertyValue);
        return buildConfig(xml, properties);
    }

    ClientConfig buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }
}

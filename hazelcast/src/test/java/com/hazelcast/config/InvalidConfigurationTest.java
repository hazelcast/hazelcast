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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_START_TAG;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvalidConfigurationTest {

    @Rule
    public ExpectedException rule = ExpectedException.none();

    @Test
    public void testWhenTwoJoinMethodEnabled() {
        expectInvalid();
        String xml = getDraftXml();
        Properties properties = getDraftProperties();
        properties.setProperty("multicast-enabled", "true");
        properties.setProperty("tcp-ip-enabled", "true");
        buildConfig(xml, properties);
    }

    @Test
    public void testWhenXmlValid() {
        String xml = getDraftXml();
        buildConfig(xml);
    }

    @Test
    public void testWhenXmlValidAndPropertiesAreResolved() {
        String xml = getDraftXml();
        Properties properties = getDraftProperties();
        buildConfig(xml, properties);
    }


    @Test
    public void testWhenInvalid_QueueBackupCount() {
        expectInvalid();
        buildConfig("queue-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_QueueBackupCount() {
        buildConfig("queue-backup-count", getValidBackupCount());
    }

    @Test
    public void testWhenInvalid_AsyncQueueBackupCount() {
        expectInvalid();
        buildConfig("queue-async-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_AsyncQueueBackupCount() {
        buildConfig("queue-async-backup-count", getValidBackupCount());
    }

    @Test
    public void testWhenValid_QueueTTL() {
        buildConfig("empty-queue-ttl", "10");
    }

    @Test
    public void testWhenInValid_QueueTTL() {
        expectInvalid();
        buildConfig("empty-queue-ttl", "a");
    }

    @Test
    public void testWhenInvalid_MapMemoryFormat() {
        expectInvalid();
        buildConfig("map-in-memory-format", "binary");
    }

    @Test
    public void testWhenInvalid_MapBackupCount() {
        expectInvalid();
        buildConfig("map-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_MapBackupCount() {
        buildConfig("map-backup-count", getValidBackupCount());
    }

    @Test
    public void testWhenInvalid_MapTTL() {
        expectInvalid();
        buildConfig("map-time-to-live-seconds", "-1");
    }

    @Test
    public void testWhenInvalid_MapMaxIdleSeconds() {
        expectInvalid();
        buildConfig("map-max-idle-seconds", "-1");
    }

    @Test
    public void testWhenInvalid_MultiMapBackupCount() {
        expectInvalid();
        buildConfig("multimap-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_MultiMapStats() {
        buildConfig("multimap-statistics-enabled", "false");
    }

    @Test
    public void testWhenValid_MultiMapBinary() {
        buildConfig("multimap-binary", "false");
    }

    @Test
    public void testWhenValid_MultiMapBackupCount() {
        buildConfig("multimap-backup-count", getValidBackupCount());
    }

    @Test
    public void testWhenInvalidValid_MultiMapCollectionType() {
        expectInvalid();
        buildConfig("multimap-value-collection-type", "set");
    }

    @Test
    public void testWhenInvalid_ListBackupCount() {
        expectInvalid();
        buildConfig("list-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_ListBackupCount() {
        buildConfig("list-backup-count", getValidBackupCount());
    }

    @Test
    public void testWhenInvalid_SetBackupCount() {
        expectInvalid();
        buildConfig("list-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_SetBackupCount() {
        buildConfig("list-backup-count", getValidBackupCount());
    }

    @Test
    public void testWhenInvalidTcpIpConfiguration() {
        expectInvalid();
        buildConfig(HAZELCAST_START_TAG
                + "<network\n>"
                + "<join>\n"
                + "<tcp-ip enabled=\"true\">\n"
                + "<required-member>127.0.0.1</required-member>\n"
                + "<required-member>128.0.0.1</required-member>\n"
                + "</tcp-ip>\n"
                + "</join>\n"
                + "</network>\n"
                + "</hazelcast>\n");
    }

    @Test
    public void invalidConfigurationTest_WhenOrderIsDifferent() {
        buildConfig(HAZELCAST_START_TAG
                + "<list name=\"default\">\n"
                + "<statistics-enabled>false</statistics-enabled>\n"
                + "<max-size>0</max-size>\n"
                + "<backup-count>1</backup-count>\n"
                + "<async-backup-count>0</async-backup-count>\n"
                + "</list>\n"
                + "</hazelcast>\n");
        buildConfig(HAZELCAST_START_TAG
                + "<list name=\"default\">\n"
                + "<backup-count>1</backup-count>\n"
                + "<async-backup-count>0</async-backup-count>\n"
                + "<statistics-enabled>false</statistics-enabled>\n"
                + "<max-size>0</max-size>\n"
                + "</list>\n"
                + "</hazelcast>\n");
    }

    @Test
    public void testWhenDocTypeAddedToXml() {
//        expectInvalid("DOCTYPE is disallowed when the feature " +
//                "\"http://apache.org/xml/features/disallow-doctype-decl\" set to true.");
        rule.expect(InvalidConfigurationException.class);
        buildConfig("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<!DOCTYPE hazelcast [ <!ENTITY e1 \"0123456789\"> ] >\n"
                + HAZELCAST_START_TAG
                + "</hazelcast>");
    }

    public void testWhenInvalid_CacheBackupCount() {
        buildConfig("cache-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_CacheBackupCount() {
        buildConfig("cache-backup-count", getValidBackupCount());
    }

    @Test
    public void testWhenInvalid_CacheAsyncBackupCount() {
        expectInvalid();
        buildConfig("cache-async-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_CacheAsyncBackupCount() {
        buildConfig("cache-async-backup-count", getValidBackupCount());
    }

    @Test
    public void testWhenValid_CacheInMemoryFormat() {
        buildConfig("cache-in-memory-format", "OBJECT");
    }

    @Test
    public void testWhenInvalid_CacheInMemoryFormat() {
        expectInvalid();
        buildConfig("cache-in-memory-format", "binaryyy");
    }

    @Test
    public void testWhenInvalid_EmptyDurationTime() {
        expectInvalid();
        buildConfig("cache-expiry-policy-duration-amount", "");
    }

    @Test
    public void testWhenInvalid_InvalidDurationTime() {
        expectInvalid();
        buildConfig("cache-expiry-policy-duration-amount", "asd");
    }

    @Test
    public void testWhenInvalid_NegativeDurationTime() {
        expectInvalid();
        buildConfig("cache-expiry-policy-duration-amount", "-1");
    }

    @Test
    public void testWhenInvalid_EmptyTimeUnit() {
        expectInvalid();
        buildConfig("cache-expiry-policy-time-unit", "");
    }

    @Test
    public void testWhenInvalid_InvalidTimeUnit() {
        expectInvalid();
        buildConfig("cache-expiry-policy-time-unit", "asd");
    }

    @Test
    public void testWhenInvalid_CacheEvictionSize() {
        expectInvalid();
        buildConfig("cache-eviction-size", "-100");
    }

    @Test
    public void testWhenValid_CacheEvictionSize() {
        buildConfig("cache-eviction-size", "100");
    }

    @Test
    public void testWhenInvalid_CacheEvictionPolicy() {
        expectInvalid();
        buildConfig("cache-eviction-policy", "NONE");
    }

    @Test
    public void testWhenInvalid_BothOfEvictionPolicyAndComparatorClassNameConfigured() {
        expectInvalid();
        Map<String, String> props = new HashMap<String, String>();
        props.put("cache-eviction-policy", "LFU");
        props.put("cache-eviction-policy-comparator-class-name", "my-comparator");

        buildConfig(props);
    }

    private static Config buildConfig(String xml) {
        return buildConfig(xml, getDraftProperties());
    }

    private static Config buildConfig(String propertyKey, String propertyValue) {
        String xml = getDraftXml();
        Properties properties = getDraftProperties();
        properties.setProperty(propertyKey, propertyValue);
        return buildConfig(xml, properties);
    }

    private static Config buildConfig(Map<String, String> props) {
        String xml = getDraftXml();
        Properties properties = getDraftProperties();
        for (Map.Entry<String, String> prop : props.entrySet()) {
            properties.setProperty(prop.getKey(), prop.getValue());
        }
        return buildConfig(xml, properties);
    }

    private void expectInvalid() {
        expectInvalid(rule);
    }

    private static Config buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    static void expectInvalid(ExpectedException rule) {
        rule.expect(InvalidConfigurationException.class);
    }

    private static String getValidBackupCount() {
        final Random random = new Random();
        return String.valueOf(random.nextInt(7));
    }

    private static String getInvalidBackupCount() {
        final Random random = new Random();
        return String.valueOf(random.nextInt(1000) + 7);
    }

    private static Properties getDraftProperties() {
        Properties properties = new Properties();

        properties.setProperty("queue-backup-count", "0");
        properties.setProperty("queue-async-backup-count", "0");

        properties.setProperty("empty-queue-ttl", "-1");

        properties.setProperty("map-in-memory-format", "BINARY");
        properties.setProperty("map-backup-count", "0");
        properties.setProperty("map-async-backup-count", "0");
        properties.setProperty("map-time-to-live-seconds", "0");
        properties.setProperty("map-max-idle-seconds", "0");
        properties.setProperty("map-eviction-policy", "NONE");

        properties.setProperty("cache-in-memory-format", "BINARY");
        properties.setProperty("cache-backup-count", "0");
        properties.setProperty("cache-async-backup-count", "0");
        properties.setProperty("cache-statistics-enabled", "true");
        properties.setProperty("cache-management-enabled", "true");
        properties.setProperty("cache-read-through", "true");
        properties.setProperty("cache-write-through", "true");
        properties.setProperty("cache-eviction-size", "100");
        properties.setProperty("cache-eviction-max-size-policy", "ENTRY_COUNT");
        properties.setProperty("cache-eviction-policy", "LRU");
        properties.setProperty("cache-eviction-policy-comparator-class-name", "");
        properties.setProperty("cache-expiry-policy-type", "CREATED");
        properties.setProperty("cache-expiry-policy-duration-amount", "1");
        properties.setProperty("cache-expiry-policy-time-unit", "DAYS");

        properties.setProperty("multimap-backup-count", "0");
        properties.setProperty("multimap-value-collection-type", "SET");
        properties.setProperty("multimap-statistics-enabled", "false");
        properties.setProperty("multimap-binary", "false");

        properties.setProperty("list-backup-count", "0");

        properties.setProperty("set-backup-count", "1");

        properties.setProperty("semaphore-initial-permits", "0");
        properties.setProperty("semaphore-backup-count", "0");
        properties.setProperty("semaphore-async-backup-count", "0");

        properties.setProperty("multicast-enabled", "false");

        properties.setProperty("tcp-ip-enabled", "false");

        return properties;
    }

    private static String getDraftXml() {
        return HAZELCAST_START_TAG
                + " <network>\n"
                + "<join>\n"
                + "<multicast enabled=\"${multicast-enabled}\">\n"
                + "</multicast>\n"
                + "<tcp-ip enabled=\"${tcp-ip-enabled}\">\n"
                + "</tcp-ip>\n"
                + "</join>\n"
                + "</network>\n"

                + "<queue name=\"default\">\n"
                + "<split-brain-protection-ref>splitBrainProtectionRuleWithThreeMembers</split-brain-protection-ref>"
                + "<backup-count>${queue-backup-count}</backup-count>\n"
                + "<async-backup-count>${queue-async-backup-count}</async-backup-count>\n"
                + "<empty-queue-ttl>${empty-queue-ttl}</empty-queue-ttl>\n"
                + "</queue>\n"

                + "<map name=\"default\">\n"
                + "<in-memory-format>${map-in-memory-format}</in-memory-format>\n"
                + "<backup-count>${map-backup-count}</backup-count>\n"
                + "<async-backup-count>${map-async-backup-count}</async-backup-count>\n"
                + "<time-to-live-seconds>${map-time-to-live-seconds}</time-to-live-seconds>\n"
                + "<max-idle-seconds>${map-max-idle-seconds}</max-idle-seconds>\n"
                + "<eviction size=\"0\" eviction-policy=\"${map-eviction-policy}\"/>"
                + "</map>\n"

                + "<cache name=\"default\">\n"
                + "<key-type class-name=\"${cache-key-type-class-name}\"/>\n"
                + "<value-type class-name=\"${cache-value-type-class-name}\"/>\n"
                + "<in-memory-format>${cache-in-memory-format}</in-memory-format>\n"
                + "<statistics-enabled>${cache-statistics-enabled}</statistics-enabled>\n"
                + "<management-enabled>${cache-management-enabled}</management-enabled>\n"
                + "<backup-count>${cache-backup-count}</backup-count>\n"
                + "<async-backup-count>${cache-async-backup-count}</async-backup-count>\n"
                + "<read-through>${cache-read-through}</read-through>\n"
                + "<write-through>${cache-write-through}</write-through>\n"
                + "<cache-loader-factory class-name=\"${cache-loader-factory-class-name}\"/>\n"
                + "<cache-writer-factory class-name=\"${cache-writer-factory-class-name}\"/>\n"
                + "<expiry-policy-factory class-name=\"${expiry-policy-factory-class-name}\"/>\n"
                + "<eviction size=\"${cache-eviction-size}\""
                + " max-size-policy=\"${cache-eviction-max-size-policy}\""
                + " eviction-policy=\"${cache-eviction-policy}\""
                + " comparator-class-name=\"${cache-eviction-policy-comparator-class-name}\"/>\n"
                + "</cache>\n"

                + "<cache name=\"cacheWithTimedExpiryPolicyFactory\">\n"
                + "<expiry-policy-factory>\n"
                + "<timed-expiry-policy-factory"
                + " expiry-policy-type=\"${cache-expiry-policy-type}\""
                + " duration-amount=\"${cache-expiry-policy-duration-amount}\""
                + " time-unit=\"${cache-expiry-policy-time-unit}\"/>"
                + "</expiry-policy-factory>\n"
                + "</cache>\n"

                + "<multimap name=\"default\">\n"
                + "<backup-count>${multimap-backup-count}</backup-count>\n"
                + "<value-collection-type>${multimap-value-collection-type}</value-collection-type>\n"
                + "<statistics-enabled>${multimap-statistics-enabled}</statistics-enabled>\n"
                + "<binary>${multimap-binary}</binary>\n"
                + "</multimap>\n"

                + "<list name=\"default\">\n"
                + "<backup-count>${list-backup-count}</backup-count>\n"
                + "</list>\n"

                + "<set name=\"default\">\n"
                + "<backup-count>${set-backup-count}</backup-count>\n"
                + "</set>\n"

                + "    <ringbuffer name=\"default\">\n"
                + "        <capacity>10000</capacity>\n"
                + "        <time-to-live-seconds>30</time-to-live-seconds>\n"
                + "        <backup-count>1</backup-count>\n"
                + "        <async-backup-count>0</async-backup-count>\n"
                + "        <in-memory-format>BINARY</in-memory-format>\n"
                + "        <ringbuffer-store enabled=\"true\">\n"
                + "            <class-name>com.hazelcast.RingbufferStoreImpl</class-name>\n"
                + "        </ringbuffer-store>\n"
                + "    </ringbuffer>"

                + "</hazelcast>\n";
    }
}

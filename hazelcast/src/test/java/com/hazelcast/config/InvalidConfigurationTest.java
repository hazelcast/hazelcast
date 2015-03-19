/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.Properties;
import java.util.Random;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvalidConfigurationTest {


    @Test(expected = InvalidConfigurationException.class)
    public void testWhenTwoJoinMethodEnabled() {
        String xml = getDraftXml();
        Properties properties = getDraftProperties();
        properties.setProperty("multicast-enabled", "true");
        properties.setProperty("tcp-ip-enabled", "true");
        buildConfig(xml, properties);
    }

    @Test
    public void testWhenXmlvalid() {
        String xml = getDraftXml();
        buildConfig(xml);
    }


    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_QueueBackupCount() {
        buildConfig("queue-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_QueueBackupCount() {
        buildConfig("queue-backup-count", getValidBackupCount());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_AsyncQueueBackupCount() {
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

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInValid_QueueTTL() {
        buildConfig("empty-queue-ttl", "a");
    }


    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInValid_MapMemoryFormat() {
        buildConfig("map-in-memory-format", "binary");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_MapBackupCount() {
        buildConfig("map-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_MapBackupCount() {
        buildConfig("map-backup-count", getValidBackupCount());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_MapTTL() {
        buildConfig("map-time-to-live-seconds", "-1");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_MapMaxIdleSeconds() {
        buildConfig("map-max-idle-seconds", "-1");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_MapEvictionPolicy() {
        buildConfig("map-eviction-policy", "none");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_MapEvictionPercentage() {
        buildConfig("map-eviction-percentage", "101");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_MultiMapBackupCount() {
        buildConfig("multimap-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_MultiMapBackupCount() {
        buildConfig("multimap-backup-count", getValidBackupCount());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalidValid_MultiMapCollectionType() {
        buildConfig("multimap-value-collection-type", "set");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_ListBackupCount() {
        buildConfig("list-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_ListBackupCount() {
        buildConfig("list-backup-count", getValidBackupCount());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_SetBackupCount() {
        buildConfig("list-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_SetBackupCount() {
        buildConfig("list-backup-count", getValidBackupCount());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_SemaphoreInitialPermits(){
        buildConfig("semaphore-initial-permits","-1");
    }
    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_SemaphoreBackupCount() {
        buildConfig("semaphore-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_SemaphoreBackupCount() {
        buildConfig("semaphore-backup-count", getValidBackupCount());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalid_AsyncSemaphoreBackupCount() {
        buildConfig("semaphore-async-backup-count", getInvalidBackupCount());
    }

    @Test
    public void testWhenValid_AsyncSemaphoreBackupCount() {
        buildConfig("semaphore-async-backup-count", getValidBackupCount());
    }


    @Test(expected = InvalidConfigurationException.class)
    public void testWhenInvalidTcpIpConfiguration() {
        String xml =
                "<hazelcast>\n" +
                        "<network\n>" +
                        "<join>\n" +
                        "<tcp-ip enabled=\"true\">\n" +
                        "<required-member>127.0.0.1</required-member>\n" +
                        "<required-member>128.0.0.1</required-member>\n" +
                        "</tcp-ip>\n" +
                        "</join>\n" +
                        "</network>\n" +
                        "</hazelcast>\n";
        buildConfig(xml);
    }

    @Test
    public void invalidConfigurationTest_WhenOrderIsDifferent() {
        String xml =
                "<hazelcast>\n" +
                        "<list name=\"default\">\n" +
                        "<statistics-enabled>false</statistics-enabled>\n" +
                        "<max-size>0</max-size>\n" +
                        "<backup-count>1</backup-count>\n" +
                        "<async-backup-count>0</async-backup-count>\n" +
                        "</list>\n" +
                        "</hazelcast>\n";
        buildConfig(xml);
        String xml2 =
                "<hazelcast>\n" +
                        "<list name=\"default\">\n" +
                        "<backup-count>1</backup-count>\n" +
                        "<async-backup-count>0</async-backup-count>\n" +
                        "<statistics-enabled>false</statistics-enabled>\n" +
                        "<max-size>0</max-size>\n" +
                        "</list>\n" +
                        "</hazelcast>\n";
        buildConfig(xml2);
    }

    String getDraftXml() {
        return
                "<hazelcast>\n" +
                        " <network>\n" +
                        "<join>\n" +
                        "<multicast enabled=\"${multicast-enabled}\">\n" +
                        "</multicast>\n" +
                        "<tcp-ip enabled=\"${tcp-ip-enabled}\">\n" +
                        "</tcp-ip>\n" +
                        "</join>\n" +
                        "</network>\n" +
                        "<queue name=\"default\">\n" +
                        "<max-size>0</max-size>\n" +
                        "<backup-count>${queue-backup-count}</backup-count>\n" +
                        "<async-backup-count>${queue-async-backup-count}</async-backup-count>\n" +
                        "<empty-queue-ttl>${empty-queue-ttl}</empty-queue-ttl>\n" +
                        "</queue>\n" +

                        "<map name=\"default\">\n" +
                        "<in-memory-format>${map-in-memory-format}</in-memory-format>\n" +
                        "<backup-count>${map-backup-count}</backup-count>\n" +
                        "<async-backup-count>${map-async-backup-count}</async-backup-count>\n" +
                        "<time-to-live-seconds>${map-time-to-live-seconds}</time-to-live-seconds>\n" +
                        "<max-idle-seconds>${map-max-idle-seconds}</max-idle-seconds>\n" +
                        "<eviction-policy>${map-eviction-policy}</eviction-policy>\n" +
                        "<eviction-percentage>${map-eviction-percentage}</eviction-percentage>\n" +
                        "</map>\n" +

                        "<multimap name=\"default\">\n" +
                        "<backup-count>${multimap-backup-count}</backup-count>\n" +
                        "<value-collection-type>${multimap-value-collection-type}</value-collection-type>\n" +
                        "</multimap>\n" +

                        "<list name=\"default\">\n" +
                        "<backup-count>${list-backup-count}</backup-count>\n" +
                        "</list>\n" +

                        "<set name=\"default\">\n" +
                        "<backup-count>${set-backup-count}</backup-count>\n" +
                        "</set>\n" +
                        "<semaphore name=\"default\">\n" +
                        "<initial-permits>${semaphore-initial-permits}</initial-permits>\n" +
                        "<backup-count>${semaphore-backup-count}</backup-count>\n" +
                        "<async-backup-count>${semaphore-async-backup-count}</async-backup-count>\n" +
                        "</semaphore>\n" +
                        "</hazelcast>\n";
    }

    Properties getDraftProperties() {
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
        properties.setProperty("map-eviction-percentage", "25");
        properties.setProperty("multimap-backup-count", "0");
        properties.setProperty("multimap-value-collection-type", "SET");
        properties.setProperty("list-backup-count", "0");
        properties.setProperty("set-backup-count", "1");
        properties.setProperty("semaphore-initial-permits", "0");
        properties.setProperty("semaphore-backup-count", "1");
        properties.setProperty("semaphore-async-backup-count", "0");
        properties.setProperty("multicast-enabled", "false");
        properties.setProperty("tcp-ip-enabled", "false");
        return properties;
    }

    Config buildConfig(String xml) {
        return buildConfig(xml, getDraftProperties());
    }

    Config buildConfig(String propertyKey, String propertyValue) {
        String xml = getDraftXml();
        Properties properties = getDraftProperties();
        properties.setProperty(propertyKey, propertyValue);
        return buildConfig(xml, properties);
    }

    Config buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    private static String getValidBackupCount() {
        final Random random = new Random();
        return String.valueOf(random.nextInt(7));
    }

    private static String getInvalidBackupCount() {
        final Random random = new Random();
        return String.valueOf(random.nextInt(1000) + 7);
    }

}

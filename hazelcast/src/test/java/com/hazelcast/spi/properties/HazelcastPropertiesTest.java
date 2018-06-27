/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.properties;

import com.hazelcast.config.Config;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.properties.GroupProperty.ENTERPRISE_LICENSE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastPropertiesTest {

    private final Config config = new Config();
    private final HazelcastProperties defaultProperties = new HazelcastProperties(config);

    @Test
    public void testNullProperties() {
        HazelcastProperties properties = new HazelcastProperties((Properties) null);

        assertTrue(properties.keySet().isEmpty());
    }

    @Test
    public void testKeySet_whenPropertiesAvailable() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        HazelcastProperties properties = new HazelcastProperties(props);

        assertEquals(props.keySet(), properties.keySet());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySet_isImmutable() {
        HazelcastProperties properties = new HazelcastProperties(config);

        properties.keySet().remove("foo");
    }

    @Test(expected = NullPointerException.class)
    public void testGet_whenKeyNull() {
        HazelcastProperties properties = new HazelcastProperties(config);

        properties.get(null);
    }

    @Test
    public void testGet_whenKeyNotExisting() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        HazelcastProperties properties = new HazelcastProperties(props);

        assertNull(properties.get("nonExistingKey"));
    }

    @Test
    public void testGet_whenKeyExisting() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        HazelcastProperties properties = new HazelcastProperties(props);

        assertEquals("value1", properties.get("key1"));
    }

    @Test
    public void setProperty_ensureHighestPriorityOfConfig() {
        config.setProperty(ENTERPRISE_LICENSE_KEY.getName(), "configValue");
        ENTERPRISE_LICENSE_KEY.setSystemProperty("systemValue");

        HazelcastProperties properties = new HazelcastProperties(config);
        String value = properties.getString(ENTERPRISE_LICENSE_KEY);

        System.clearProperty(ENTERPRISE_LICENSE_KEY.getName());

        assertEquals("configValue", value);
    }

    @Test
    public void setProperty_ensureUsageOfSystemProperty() {
        ENTERPRISE_LICENSE_KEY.setSystemProperty("systemValue");

        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        String value = hazelcastProperties.getString(ENTERPRISE_LICENSE_KEY);

        System.clearProperty(ENTERPRISE_LICENSE_KEY.getName());

        assertEquals("systemValue", value);
    }

    @Test
    public void setProperty_ensureUsageOfDefaultValue() {
        String value = defaultProperties.getString(ENTERPRISE_LICENSE_KEY);

        assertNull(value);
    }

    @Test
    public void setProperty_inheritDefaultValueOfParentProperty() {
        String inputIOThreadCount = defaultProperties.getString(GroupProperty.IO_INPUT_THREAD_COUNT);

        assertEquals(GroupProperty.IO_THREAD_COUNT.getDefaultValue(), inputIOThreadCount);
    }

    @Test
    public void setProperty_inheritActualValueOfParentProperty() {
        config.setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        HazelcastProperties properties = new HazelcastProperties(config);

        String inputIOThreadCount = properties.getString(GroupProperty.IO_INPUT_THREAD_COUNT);

        assertEquals("1", inputIOThreadCount);
        assertNotEquals(GroupProperty.IO_THREAD_COUNT.getDefaultValue(), inputIOThreadCount);
    }

    @Test
    public void getSystemProperty() {
        GroupProperty.APPLICATION_VALIDATION_TOKEN.setSystemProperty("token");

        assertEquals("token", GroupProperty.APPLICATION_VALIDATION_TOKEN.getSystemProperty());

        System.clearProperty(GroupProperty.APPLICATION_VALIDATION_TOKEN.getName());
    }

    @Test
    public void getBoolean() {
        HazelcastProperty property = new HazelcastProperty("foo", "true");

        boolean isHumanReadable = defaultProperties.getBoolean(property);

        assertTrue(isHumanReadable);
    }

    @Test
    public void getInteger() {
        int ioThreadCount = defaultProperties.getInteger(GroupProperty.IO_THREAD_COUNT);

        assertEquals(3, ioThreadCount);
    }

    @Test
    public void getLong() {
        long lockMaxLeaseTimeSeconds = defaultProperties.getLong(GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS);

        assertEquals(Long.MAX_VALUE, lockMaxLeaseTimeSeconds);
    }

    @Test
    public void getFloat() {
        HazelcastProperty property = new HazelcastProperty("foo", "10");

        float maxFileSize = defaultProperties.getFloat(property);

        assertEquals(10, maxFileSize, 0.0001);
    }

    @Test
    public void getPositiveMillisOrDefault() {
        String name = GroupProperty.PARTITION_TABLE_SEND_INTERVAL.getName();
        config.setProperty(name, "-300");
        HazelcastProperty property = new HazelcastProperty(name, "20", TimeUnit.MILLISECONDS);

        long millis = defaultProperties.getPositiveMillisOrDefault(property);

        assertEquals(20, millis);
    }

    @Test
    public void getPositiveMillisOrDefaultWithManualDefault() {
        String name = GroupProperty.PARTITION_TABLE_SEND_INTERVAL.getName();
        config.setProperty(name, "-300");
        HazelcastProperties properties = new HazelcastProperties(config);
        HazelcastProperty property = new HazelcastProperty(name, "20", TimeUnit.MILLISECONDS);

        long millis = properties.getPositiveMillisOrDefault(property, 50);

        assertEquals(50, millis);
    }

    @Test
    public void getTimeUnit() {
        config.setProperty(GroupProperty.PARTITION_TABLE_SEND_INTERVAL.getName(), "300");
        HazelcastProperties properties = new HazelcastProperties(config);

        assertEquals(300, properties.getSeconds(GroupProperty.PARTITION_TABLE_SEND_INTERVAL));
    }

    @Test
    public void getTimeUnit_default() {
        long expectedSeconds = 15;

        long intervalNanos = defaultProperties.getNanos(GroupProperty.PARTITION_TABLE_SEND_INTERVAL);
        long intervalMillis = defaultProperties.getMillis(GroupProperty.PARTITION_TABLE_SEND_INTERVAL);
        long intervalSeconds = defaultProperties.getSeconds(GroupProperty.PARTITION_TABLE_SEND_INTERVAL);

        assertEquals(TimeUnit.SECONDS.toNanos(expectedSeconds), intervalNanos);
        assertEquals(TimeUnit.SECONDS.toMillis(expectedSeconds), intervalMillis);
        assertEquals(expectedSeconds, intervalSeconds);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTimeUnit_noTimeUnitProperty() {
        defaultProperties.getMillis(GroupProperty.APPLICATION_VALIDATION_TOKEN);
    }

    @Test
    public void getEnum() {
        config.setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.DEBUG.toString());
        HazelcastProperties properties = new HazelcastProperties(config);

        ProbeLevel level = properties.getEnum(Diagnostics.METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.DEBUG, level);
    }

    @Test
    public void getEnum_default() {
        ProbeLevel level = defaultProperties.getEnum(Diagnostics.METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.MANDATORY, level);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEnum_nonExistingEnum() {
        config.setProperty(Diagnostics.METRICS_LEVEL.getName(), "notExist");
        HazelcastProperties properties = new HazelcastProperties(config);
        properties.getEnum(Diagnostics.METRICS_LEVEL, ProbeLevel.class);
    }

    @Test
    public void getEnum_ignoredName() {
        config.setProperty(Diagnostics.METRICS_LEVEL.getName(), "dEbUg");
        HazelcastProperties properties = new HazelcastProperties(config);

        ProbeLevel level = properties.getEnum(Diagnostics.METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.DEBUG, level);
    }

    @Test
    public void getString_whenDeprecatedNameUsed() {
        Properties props = new Properties();
        props.setProperty("oldname", "10");
        HazelcastProperties properties = new HazelcastProperties(props);

        HazelcastProperty property = new HazelcastProperty("newname")
                .setDeprecatedName("oldname");
        String value = properties.getString(property);

        assertEquals("10", value);
    }
}

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

package com.hazelcast.spi.properties;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import com.hazelcast.config.Config;
import com.hazelcast.internal.diagnostics.HealthMonitorLevel;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.net.URL;

import static com.hazelcast.spi.properties.ClusterProperty.ENTERPRISE_LICENSE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.StringUtils;

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
    public void testGet_whenValueNotString() {
        // given a "compromised" Properties object
        Properties props = new Properties();
        props.put("key", 1);

        // HazelcastProperties.get returns null
        HazelcastProperties hzProperties = new HazelcastProperties(props);
        assertNull(hzProperties.get("key"));
    }

    @Test
    public void testGet_whenFunctionAvailable_andNoOtherSettings() {
        Properties props = new Properties();
        HazelcastProperty p = new HazelcastProperty("key", new Function<HazelcastProperties, Integer>() {
            @Override
            public Integer apply(HazelcastProperties properties) {
                return 23;
            }
        });
        HazelcastProperties properties = new HazelcastProperties(props);

        assertEquals(23, properties.getInteger(p));
    }

    @Test
    public void testGet_whenFunctionAvailable_andPropertySet() {
        Properties props = new Properties();
        props.setProperty("key", "1");
        HazelcastProperty p = new HazelcastProperty("key", new Function<HazelcastProperties, Integer>() {
            @Override
            public Integer apply(HazelcastProperties properties) {
                return 23;
            }
        });
        HazelcastProperties properties = new HazelcastProperties(props);
        assertEquals(1, properties.getInteger(p));
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
        HazelcastProperty parent = new HazelcastProperty("parent", 1);
        HazelcastProperty child = new HazelcastProperty("child", parent);

        assertEquals(1, defaultProperties.getInteger(child));
    }

    @Test
    public void setProperty_inheritActualValueOfParentProperty() {
        config.setProperty(ClusterProperty.IO_THREAD_COUNT.getName(), "1");
        HazelcastProperties properties = new HazelcastProperties(config);

        String inputIOThreadCount = properties.getString(ClusterProperty.IO_INPUT_THREAD_COUNT);

        assertEquals("1", inputIOThreadCount);
        assertNotEquals(ClusterProperty.IO_THREAD_COUNT.getDefaultValue(), inputIOThreadCount);
    }

    @Test
    public void getSystemProperty() {
        ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.setSystemProperty("12");

        assertEquals("12", ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getSystemProperty());

        System.clearProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName());
    }

    @Test
    public void getBoolean() {
        HazelcastProperty property = new HazelcastProperty("foo", "true");

        boolean isHumanReadable = defaultProperties.getBoolean(property);

        assertTrue(isHumanReadable);
    }

    @Test
    public void getInteger() {
        HazelcastProperty property = new HazelcastProperty("key", 3);
        int ioThreadCount = defaultProperties.getInteger(property);

        assertEquals(3, ioThreadCount);
    }

    @Test
    public void getLong() {
        long lockMaxLeaseTimeSeconds = defaultProperties.getLong(ClusterProperty.LOCK_MAX_LEASE_TIME_SECONDS);

        assertEquals(Long.MAX_VALUE, lockMaxLeaseTimeSeconds);
    }

    @Test
    public void getFloat() {
        HazelcastProperty property = new HazelcastProperty("foo", 10.1F);

        float foo = defaultProperties.getFloat(property);

        assertEquals(10.1F, foo, 0.0001);
    }

    @Test
    public void getDouble() {
        HazelcastProperty property = new HazelcastProperty("foo", 10.1D);
        double foo = defaultProperties.getDouble(property);

        assertEquals(10.1D, foo, 0.0001);
    }

    @Test
    public void getPositiveMillisOrDefault() {
        String name = ClusterProperty.PARTITION_TABLE_SEND_INTERVAL.getName();
        config.setProperty(name, "-300");
        HazelcastProperty property = new HazelcastProperty(name, "20", TimeUnit.MILLISECONDS);

        long millis = defaultProperties.getPositiveMillisOrDefault(property);

        assertEquals(20, millis);
    }

    @Test
    public void getPositiveMillisOrDefaultWithManualDefault() {
        String name = ClusterProperty.PARTITION_TABLE_SEND_INTERVAL.getName();
        config.setProperty(name, "-300");
        HazelcastProperties properties = new HazelcastProperties(config);
        HazelcastProperty property = new HazelcastProperty(name, "20", TimeUnit.MILLISECONDS);

        long millis = properties.getPositiveMillisOrDefault(property, 50);

        assertEquals(50, millis);
    }

    @Test
    public void getTimeUnit() {
        config.setProperty(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL.getName(), "300");
        HazelcastProperties properties = new HazelcastProperties(config);

        assertEquals(300, properties.getSeconds(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL));
    }

    @Test
    public void getTimeUnit_default() {
        long expectedSeconds = 15;

        long intervalNanos = defaultProperties.getNanos(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL);
        long intervalMillis = defaultProperties.getMillis(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL);
        long intervalSeconds = defaultProperties.getSeconds(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL);

        assertEquals(TimeUnit.SECONDS.toNanos(expectedSeconds), intervalNanos);
        assertEquals(TimeUnit.SECONDS.toMillis(expectedSeconds), intervalMillis);
        assertEquals(expectedSeconds, intervalSeconds);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTimeUnit_noTimeUnitProperty() {
        defaultProperties.getMillis(ClusterProperty.EVENT_THREAD_COUNT);
    }

    @Test
    public void getEnum() {
        config.setProperty(ClusterProperty.HEALTH_MONITORING_LEVEL.getName(), "NOISY");
        HazelcastProperties properties = new HazelcastProperties(config.getProperties());
        HealthMonitorLevel healthMonitorLevel = properties
                .getEnum(ClusterProperty.HEALTH_MONITORING_LEVEL, HealthMonitorLevel.class);

        assertEquals(HealthMonitorLevel.NOISY, healthMonitorLevel);
    }

    @Test
    public void getEnum_default() {
        HazelcastProperties properties = new HazelcastProperties(config.getProperties());
        HealthMonitorLevel healthMonitorLevel = properties
                .getEnum(ClusterProperty.HEALTH_MONITORING_LEVEL, HealthMonitorLevel.class);

        assertEquals(HealthMonitorLevel.SILENT, healthMonitorLevel);
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

    /**
     * Prints to the console any {@link HazelcastProperty}'s where an alternative British English version of their
     * {@link HazelcastProperty#name} or {@link HazelcastProperty#deprecatedName} exists.
     * <p>
     * e.g. {@code hazelcast.enterprise.license.key} vs {@code hazelcast.enterprise.licence.key}
     *
     * @see <a href="https://github.com/hazelcast/hazelcast/pull/25460">Print a warning when British spelling of "License" is
     *      used</a>
     */
    public static void main(final String[] args) throws Exception {
        // Load translation data
        // https://github.com/hyperreality/American-British-English-Translator/
        final Map<String, String> rawMap = new ObjectMapper().readValue(new URL(
                "https://raw.githubusercontent.com/hyperreality/American-British-English-Translator/master/data/american_spellings.json"),
                new TypeReference<Map<String, String>>() {
                });

        final Map<String, String> americanToBritish = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        americanToBritish.putAll(rawMap);

        final UnaryOperator<String> translateStringToBritishEnglish = str -> {
            final String translated = americanToBritish.get(str);

            if (translated == null) {
                return str;
            } else {
                // Highlight difference in red
                return "\u001B[31m" + translated + "\u001B[0m";
            }
        };

        final Pattern delimiterPattern = Pattern.compile("([^\\w])");
        final Function<HazelcastProperty, String> translatePropertyToBritishEnglish = property -> {
            // Generate a British English version of the properties' name
            final Matcher matcher = delimiterPattern.matcher(property.getName());
            final StringBuffer builder = new StringBuffer();

            int prevEnd = 0;
            while (matcher.find()) {
                builder.append(translateStringToBritishEnglish.apply(property.getName().substring(prevEnd, matcher.start())))
                        .append(matcher.group(1));
                prevEnd = matcher.end();
            }
            builder.append(translateStringToBritishEnglish.apply(property.getName().substring(prevEnd)));

            return builder.toString();
        };

        // Ideally you'd use the Reflections library to do this, but this struggles to return *everything*:
        // https://github.com/ronmamo/reflections/issues/385
        // Instead use Guava:
        // https://www.baeldung.com/java-find-all-classes-in-package#3-google-guava-library
        final Stream<Class<?>> allHazelcastClasses = ClassPath.from(ClassLoader.getSystemClassLoader()).getAllClasses()
                .parallelStream().filter(clazz -> clazz.getPackageName().startsWith("com.hazelcast")).map(ClassInfo::load);

        final Stream<HazelcastProperty> hazelcastProperties = allHazelcastClasses.map(Class::getDeclaredFields)
                .flatMap(Arrays::stream).filter(field -> HazelcastProperty.class.isAssignableFrom(field.getType())
                        && Modifier.isStatic(field.getModifiers()))
                .map(field -> {
                    try {
                        field.setAccessible(true);
                        return field.get(null);
                    } catch (final ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                }).map(HazelcastProperty.class::cast);

        // Print the output
        int columnWidth = 50;
        final String format = "%-" + columnWidth + "s | %-" + columnWidth + "s%n";
        System.out.printf(format, "Property Name", "Suggested British English Property Name");
        System.out.printf(format, StringUtils.repeat("-", columnWidth), StringUtils.repeat("-", columnWidth));

        hazelcastProperties.sorted(Comparator.comparing(HazelcastProperty::getName)).forEachOrdered(property -> {
            final String britishEnglishPropertyTranslation = translatePropertyToBritishEnglish.apply(property);

            // If the British English translation differs from the properties' name / deprecated name, log it
            if (!StringUtils.equalsIgnoreCase(property.getName(), britishEnglishPropertyTranslation)
                    && !StringUtils.equalsIgnoreCase(property.getDeprecatedName(), britishEnglishPropertyTranslation)) {
                System.out.printf(format, property.toString(), britishEnglishPropertyTranslation);
            }
        });
    }
}

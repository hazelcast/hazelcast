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

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;

/**
 * Container for configured Hazelcast properties ({@see HazelcastProperty}).
 * <p>
 * A {@link HazelcastProperty} can be set as:
 * <ul>
 * <li>an environmental variable using {@link System#setProperty(String, String)}</li>
 * <li>the programmatic configuration using {@link Config#setProperty(String, String)}</li>
 * <li>the XML configuration</li>
 * </ul>
 *
 * @see <a href="http://docs.hazelcast.org/docs/latest-dev/manual/html-single/hazelcast-documentation.html#system-properties">
 * System properties documentaiton</a>
 */
public class HazelcastProperties {

    private final Set<String> keys;
    private final Properties properties = new Properties();

    /**
     * Creates a container with configured Hazelcast properties.
     * <p>
     * Uses the environmental value if no value is defined in the configuration.
     * Uses the default value if no environmental value is defined.
     *
     * @param config {@link Config} used to configure the {@link HazelcastProperty} values;
     *               properties in config are allowed to be {@code null}
     */
    public HazelcastProperties(Config config) {
        this(config.getProperties());
    }

    /**
     * Creates a container with configured Hazelcast properties.
     * <p>
     * Uses the environmental value if no value is defined in the configuration.
     * Uses the default value if no environmental value is defined.
     *
     * @param nullableProperties {@link Properties} used to configure the {@link HazelcastProperty} values;
     *                           properties are allowed to be {@code null}
     */
    @SuppressWarnings("unchecked")
    public HazelcastProperties(Properties nullableProperties) {
        if (nullableProperties != null) {
            properties.putAll(nullableProperties);
        }

        this.keys = unmodifiableSet((Set) properties.keySet());
    }

    /**
     * Returns an immutable set of all keys in this HazelcastProperties.
     *
     * @return set of keys
     */
    public Set<String> keySet() {
        return keys;
    }

    /**
     * Returns the value for the given key.
     *
     * @param key the key
     * @return the value for the given key, or {@code null} if no value is found
     * @throws NullPointerException if key is {@code null}
     */
    public String get(String key) {
        return (String) properties.get(key);
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} as String.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value or {@code null} if nothing has been configured
     */
    public String getString(HazelcastProperty property) {
        String value = properties.getProperty(property.getName());
        if (value != null) {
            return value;
        }

        value = property.getSystemProperty();
        if (value != null) {
            return value;
        }

        HazelcastProperty parent = property.getParent();
        if (parent != null) {
            return getString(parent);
        }

        String deprecatedName = property.getDeprecatedName();
        if (deprecatedName != null) {
            value = get(deprecatedName);
            if (value == null) {
                value = System.getProperty(deprecatedName);
            }

            if (value != null) {
                // we don't have a logger available, and the Logging service is constructed after the Properties are created.
                System.err.print("Don't use deprecated '" + deprecatedName + "' "
                        + "but use '" + property.getName() + "' instead. "
                        + "The former name will be removed in the next Hazelcast release.");

                return value;
            }
        }

        return property.getDefaultValue();
    }

    /**
     * Returns the configured boolean value of a {@link HazelcastProperty}.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value as boolean
     */
    public boolean getBoolean(HazelcastProperty property) {
        return Boolean.valueOf(getString(property));
    }

    /**
     * Returns the configured int value of a {@link HazelcastProperty}.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value as int
     * @throws NumberFormatException if the value cannot be parsed
     */
    public int getInteger(HazelcastProperty property) {
        return Integer.parseInt(getString(property));
    }

    /**
     * Returns the configured long value of a {@link HazelcastProperty}.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value as long
     * @throws NumberFormatException if the value cannot be parsed
     */
    public long getLong(HazelcastProperty property) {
        return Long.parseLong(getString(property));
    }

    /**
     * Returns the configured float value of a {@link HazelcastProperty}.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value as float
     * @throws NumberFormatException if the value cannot be parsed
     */
    public float getFloat(HazelcastProperty property) {
        return Float.valueOf(getString(property));
    }

    /**
     * Returns the configured double value of a {@link HazelcastProperty}.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value as double
     * @throws NumberFormatException if the value cannot be parsed
     */
    public double getDouble(HazelcastProperty property) {
        return Double.valueOf(getString(property));
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} converted to nanoseconds.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value in nanoseconds
     * @throws IllegalArgumentException if the {@link HazelcastProperty} has no {@link TimeUnit}
     */
    public long getNanos(HazelcastProperty property) {
        TimeUnit timeUnit = property.getTimeUnit();
        return timeUnit.toNanos(getLong(property));
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} converted to milliseconds.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value in milliseconds
     * @throws IllegalArgumentException if the {@link HazelcastProperty} has no {@link TimeUnit}
     */
    public long getMillis(HazelcastProperty property) {
        TimeUnit timeUnit = property.getTimeUnit();
        return timeUnit.toMillis(getLong(property));
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} converted to seconds.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value in seconds
     * @throws IllegalArgumentException if the {@link HazelcastProperty} has no {@link TimeUnit}
     */
    public int getSeconds(HazelcastProperty property) {
        TimeUnit timeUnit = property.getTimeUnit();
        return (int) timeUnit.toSeconds(getLong(property));
    }

    /**
     * Returns the configured enum value of a {@link GroupProperty}.
     * <p>
     * The case of the enum is ignored.
     *
     * @param property the {@link GroupProperty} to get the value from
     * @return the enum
     * @throws IllegalArgumentException if the enum value can't be found
     */
    public <E extends Enum> E getEnum(HazelcastProperty property, Class<E> enumClazz) {
        String value = getString(property);

        for (E enumConstant : enumClazz.getEnumConstants()) {
            if (enumConstant.name().equalsIgnoreCase(value)) {
                return enumConstant;
            }
        }

        throw new IllegalArgumentException(format("value '%s' for property '%s' is not a valid %s value",
                value, property.getName(), enumClazz.getName()));
    }
}

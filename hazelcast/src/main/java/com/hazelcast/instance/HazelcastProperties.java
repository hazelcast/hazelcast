/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.spi.properties.GroupProperty;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableSet;

/**
 * Container for configured Hazelcast properties ({@see HazelcastProperty}).
 * <p/>
 * Deprecated since Hazelcast 3.7.
 * This is private API, don't use it.
 */
@Deprecated
@SuppressWarnings("unused")
public abstract class HazelcastProperties {

    private final Set<String> keys;
    private final Properties properties = new Properties();

    /**
     * Creates a container with configured Hazelcast properties.
     * <p/>
     * Uses the environmental value if no value is defined in the configuration.
     * Uses the default value if no environmental value is defined.
     *
     * @param nullableProperties  {@link Properties} used to configure the {@link HazelcastProperty} values.
     *                            Properties are allowed to be null.
     * @param hazelcastProperties array of {@link HazelcastProperty} to configure
     */
    protected HazelcastProperties(Properties nullableProperties, HazelcastProperty[] hazelcastProperties) {
        this(nullableProperties);
    }

    /**
     * Creates a container with configured Hazelcast properties.
     * <p/>
     * Uses the environmental value if no value is defined in the configuration.
     * Uses the default value if no environmental value is defined.
     *
     * @param nullableProperties {@link Properties} used to configure the {@link HazelcastProperty} values.
     *                           Properties are allowed to be null.
     */
    protected HazelcastProperties(Properties nullableProperties) {
        if (nullableProperties != null) {
            properties.putAll(nullableProperties);
        }

        this.keys = unmodifiableSet((Set) properties.keySet());
    }

    /**
     * Returns an immutable set of all keys in this HazelcastProperties.
     *
     * @return set of keys.
     */
    public Set<String> keySet() {
        return keys;
    }

    /**
     * Returns the value for the given key.
     *
     * @param key the key
     * @return the value for the given key, or null if no value is found.
     * @throws NullPointerException if key is null.
     */
    public String get(String key) {
        return (String) properties.get(key);
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} as String.
     *
     * @param property the {@link HazelcastProperty} to get the value from
     * @return the value or <tt>null</tt> if nothing has been configured
     */
    public String getString(HazelcastProperty property) {
        String configValue = properties.getProperty(property.getName());
        if (configValue != null) {
            return configValue;
        }
        String systemProperty = property.getSystemProperty();
        if (systemProperty != null) {
            return systemProperty;
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
     * <p/>
     * The case of the enum is ignored.
     *
     * @param property the {@link GroupProperty} to get the value from
     * @return the enum
     * @throws IllegalArgumentException if the enum value can't be found
     */
    public <E extends Enum> E getEnum(GroupProperty property, Class<E> enumClazz) {
        throw new UnsupportedOperationException();
    }
}

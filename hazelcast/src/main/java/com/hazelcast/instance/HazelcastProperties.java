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

package com.hazelcast.instance;

import com.hazelcast.config.Config;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Container for configured Hazelcast properties ({@see HazelcastProperty}).
 * <p/>
 * A {@link HazelcastProperty} can be set as:
 * <p><ul>
 * <li>an environmental variable using {@link System#setProperty(String, String)}</li>
 * <li>the programmatic configuration using {@link Config#setProperty(String, String)}</li>
 * <li>the XML configuration
 * {@see http://docs.hazelcast.org/docs/latest-dev/manual/html-single/hazelcast-documentation.html#system-properties}</li>
 * </ul></p>
 */
public abstract class HazelcastProperties {

    private final String[] properties = createProperties();

    /**
     * Created the properties array.
     *
     * @return a String array with the size of the properties.
     */
    protected abstract String[] createProperties();

    /**
     * Creates a container with configured Hazelcast properties.
     * <p/>
     * Uses the environmental value if no value is defined in the configuration.
     * Uses the default value if no environmental value is defined.
     *
     * @param properties          {@link Properties} used to configure the {@link HazelcastProperty} values.
     * @param hazelcastProperties array of {@link HazelcastProperty} to configure
     */
    protected void initProperties(Properties properties, HazelcastProperty[] hazelcastProperties) {
        for (HazelcastProperty property : hazelcastProperties) {
            String configValue = (properties != null) ? properties.getProperty(property.getName()) : null;
            if (configValue != null) {
                this.properties[property.getIndex()] = configValue;
                continue;
            }
            String propertyValue = property.getSystemProperty();
            if (propertyValue != null) {
                this.properties[property.getIndex()] = propertyValue;
                continue;
            }
            GroupProperty parent = property.getParent();
            if (parent != null) {
                this.properties[property.getIndex()] = this.properties[parent.ordinal()];
                continue;
            }
            this.properties[property.getIndex()] = property.getDefaultValue();
        }
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} as String.
     *
     * @param groupProperty the {@link HazelcastProperty} to get the value from
     * @return the value or <tt>null</tt> if nothing has been configured
     */
    public String getString(HazelcastProperty groupProperty) {
        return properties[groupProperty.getIndex()];
    }

    /**
     * Returns the configured boolean value of a {@link HazelcastProperty}.
     *
     * @param groupProperty the {@link HazelcastProperty} to get the value from
     * @return the value as boolean
     */
    public boolean getBoolean(HazelcastProperty groupProperty) {
        return Boolean.valueOf(properties[groupProperty.getIndex()]);
    }

    /**
     * Returns the configured int value of a {@link HazelcastProperty}.
     *
     * @param groupProperty the {@link HazelcastProperty} to get the value from
     * @return the value as int
     * @throws NumberFormatException if the value cannot be parsed
     */
    public int getInteger(HazelcastProperty groupProperty) {
        return Integer.parseInt(properties[groupProperty.getIndex()]);
    }

    /**
     * Returns the configured long value of a {@link HazelcastProperty}.
     *
     * @param groupProperty the {@link HazelcastProperty} to get the value from
     * @return the value as long
     * @throws NumberFormatException if the value cannot be parsed
     */
    public long getLong(HazelcastProperty groupProperty) {
        return Long.parseLong(properties[groupProperty.getIndex()]);
    }

    /**
     * Returns the configured float value of a {@link HazelcastProperty}.
     *
     * @param groupProperty the {@link HazelcastProperty} to get the value from
     * @return the value as float
     * @throws NumberFormatException if the value cannot be parsed
     */
    public float getFloat(HazelcastProperty groupProperty) {
        return Float.valueOf(properties[groupProperty.getIndex()]);
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} converted to nanoseconds.
     *
     * @param groupProperty the {@link HazelcastProperty} to get the value from
     * @return the value in nanoseconds
     * @throws IllegalArgumentException if the {@link HazelcastProperty} has no {@link TimeUnit}
     */
    public long getNanos(HazelcastProperty groupProperty) {
        TimeUnit timeUnit = groupProperty.getTimeUnit();
        return timeUnit.toNanos(getLong(groupProperty));
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} converted to milliseconds.
     *
     * @param groupProperty the {@link HazelcastProperty} to get the value from
     * @return the value in milliseconds
     * @throws IllegalArgumentException if the {@link HazelcastProperty} has no {@link TimeUnit}
     */
    public long getMillis(HazelcastProperty groupProperty) {
        TimeUnit timeUnit = groupProperty.getTimeUnit();
        return timeUnit.toMillis(getLong(groupProperty));
    }

    /**
     * Returns the configured value of a {@link HazelcastProperty} converted to seconds.
     *
     * @param groupProperty the {@link HazelcastProperty} to get the value from
     * @return the value in seconds
     * @throws IllegalArgumentException if the {@link HazelcastProperty} has no {@link TimeUnit}
     */
    public int getSeconds(HazelcastProperty groupProperty) {
        TimeUnit timeUnit = groupProperty.getTimeUnit();
        return (int) timeUnit.toSeconds(getLong(groupProperty));
    }

    /**
     * Returns the configured enum value of a {@link GroupProperty}.
     * <p/>
     * The case of the enum is ignored.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the enum
     * @throws IllegalArgumentException if the enum value can't be found
     */
    public <E extends Enum> E getEnum(GroupProperty groupProperty, Class<E> enumClazz) {
        String value = getString(groupProperty);

        for (E enumConstant : enumClazz.getEnumConstants()) {
            if (enumConstant.name().equalsIgnoreCase(value)) {
                return enumConstant;
            }
        }

        throw new IllegalArgumentException(format("value '%s' for property '%s' is not a valid %s value",
                value, groupProperty.getName(), enumClazz.getName()));
    }
}

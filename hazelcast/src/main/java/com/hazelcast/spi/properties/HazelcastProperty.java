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

package com.hazelcast.spi.properties;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static java.lang.String.format;

/**
 * Interface for Hazelcast Member and Client properties.
 */
public final class HazelcastProperty {

    private final String name;
    private final String defaultValue;
    private final TimeUnit timeUnit;
    private final HazelcastProperty parent;
    private final Function<HazelcastProperties, ?> function;
    private volatile String deprecatedName;

    public HazelcastProperty(String name) {
        this(name, (String) null);
    }

    public HazelcastProperty(String name, Function<HazelcastProperties, ?> function) {
        checkHasText(name, "The property name cannot be null or empty!");
        this.name = name;
        this.function = function;
        this.defaultValue = null;
        this.deprecatedName = null;
        this.parent = null;
        this.timeUnit = null;
    }

    public HazelcastProperty(String name, Enum<?> defaultEnum) {
        this(name, defaultEnum.name());
    }

    public HazelcastProperty(String name, boolean defaultValue) {
        this(name, defaultValue ? "true" : "false");
    }

    public HazelcastProperty(String name, Integer defaultValue) {
        this(name, String.valueOf(defaultValue));
    }

    public HazelcastProperty(String name, Double defaultValue) {
        this(name, String.valueOf(defaultValue));
    }

    public HazelcastProperty(String name, Float defaultValue) {
        this(name, String.valueOf(defaultValue));
    }

    public HazelcastProperty(String name, Byte defaultValue) {
        this(name, String.valueOf(defaultValue));
    }

    public HazelcastProperty(String name, Integer defaultValue, TimeUnit timeUnit) {
        this(name, String.valueOf(defaultValue), timeUnit);
    }

    public HazelcastProperty(String name, Long defaultValue, TimeUnit timeUnit) {
        this(name, Long.toString(defaultValue), timeUnit);
    }

    public HazelcastProperty(String name, HazelcastProperty property) {
        this(name, property.getDefaultValue(), property.timeUnit, property);
    }

    public HazelcastProperty(String name, String defaultValue) {
        this(name, defaultValue, null);
    }

    protected HazelcastProperty(String name, String defaultValue, TimeUnit timeUnit) {
        this(name, defaultValue, timeUnit, null);
    }

    public HazelcastProperty(String name, String defaultValue, TimeUnit timeUnit, HazelcastProperty parent) {
        checkHasText(name, "The property name cannot be null or empty!");
        this.name = name;
        this.defaultValue = defaultValue;
        this.function = null;
        this.timeUnit = timeUnit;
        this.parent = parent;
    }

    /**
     * Sets the deprecated name of ths property. Useful if compatibility needs to be provided on property names.
     *
     * This method is thread-safe, but is expected to be called immediately after the HazelcastProperty is constructed.
     *
     * <code>
     * HazelcastProperty property = new HazelcastProperty("newname").setDeprecatedName("oldname");
     * </code>
     *
     * @param deprecatedName the deprecated name of the property
     * @return the updated {@link HazelcastProperty}
     * @throws IllegalArgumentException if the deprecatedName is null or an empty string.
     */
    public HazelcastProperty setDeprecatedName(String deprecatedName) {
        this.deprecatedName = checkHasText(deprecatedName, "a valid string should be provided");
        return this;
    }

    public Function<HazelcastProperties, ?> getFunction() {
        return function;
    }

    public String getDeprecatedName() {
        return deprecatedName;
    }

    /**
     * Returns the property name.
     *
     * @return the property name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the default value of the property.
     *
     * @return the default value or <code>null</code> if none is defined
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Returns the {@link TimeUnit} of the property.
     *
     * @return the {@link TimeUnit}
     * @throws IllegalArgumentException if no {@link TimeUnit} is defined
     */
    public TimeUnit getTimeUnit() {
        if (timeUnit == null) {
            throw new IllegalArgumentException(format("Cluster property %s has no TimeUnit defined!", this));
        }

        return timeUnit;
    }

    /**
     * Returns the parent {@link ClusterProperty} of the property.
     *
     * @return the parent {@link ClusterProperty} or <code>null</code> if none is defined
     */
    public HazelcastProperty getParent() {
        return parent;
    }

    /**
     * Sets the system property value of the property.
     *
     * @param value the value to set
     */
    public void setSystemProperty(String value) {
        System.setProperty(name, value);
    }

    /**
     * Gets the system property value of the property.
     *
     * @return the value of the property
     */
    public String getSystemProperty() {
        return System.getProperty(name);
    }

    @Override
    public String toString() {
        return name;
    }
}

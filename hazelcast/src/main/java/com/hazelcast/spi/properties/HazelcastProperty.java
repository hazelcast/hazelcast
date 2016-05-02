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

package com.hazelcast.spi.properties;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkHasText;
import static java.lang.String.format;

/**
 * Interface for Hazelcast Member and Client properties.
 */
public final class HazelcastProperty {

    private final String name;
    private final String defaultValue;
    private final TimeUnit timeUnit;
    private final HazelcastProperty parent;

    public HazelcastProperty(String name) {
        this(name, (String) null);
    }

    public HazelcastProperty(String name, boolean defaultValue) {
        this(name, defaultValue ? "true" : "false");
    }

    public HazelcastProperty(String name, Integer defaultValue) {
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

    public HazelcastProperty(String name, HazelcastProperty groupProperty) {
        this(name, groupProperty.getDefaultValue(), groupProperty.timeUnit, groupProperty);
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
        this.timeUnit = timeUnit;
        this.parent = parent;
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
     * @return the default value or <tt>null</tt> if none is defined
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
            throw new IllegalArgumentException(format("groupProperty %s has no TimeUnit defined!", this));
        }

        return timeUnit;
    }

    /**
     * Returns the parent {@link GroupProperty} of the property.
     *
     * @return the parent {@link GroupProperty} or <tt>null</tt> if none is defined
     */
    public HazelcastProperty getParent() {
        return parent;
    }


    /**
     * Sets the environmental value of the property.
     *
     * @param value the value to set
     */
    public void setSystemProperty(String value) {
        System.setProperty(name, value);
    }

    /**
     * Gets the environmental value of the property.
     *
     * @return the value of the property
     */

    public String getSystemProperty() {
        return System.getProperty(name);
    }

    /**
     * Clears the environmental value of the property.
     */
    public String clearSystemProperty() {
        return System.clearProperty(name);
    }

    @Override
    public String toString() {
        return name;
    }
}

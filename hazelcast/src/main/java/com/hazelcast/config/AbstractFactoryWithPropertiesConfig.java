/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.util.Preconditions.checkHasText;

import java.util.Objects;
import java.util.Properties;

import javax.annotation.Nonnull;

/**
 * Configuration base for config types with a factory class and its properties.
 *
 * @param <T> final child type
 */
public abstract class AbstractFactoryWithPropertiesConfig<T extends AbstractFactoryWithPropertiesConfig<T>> {

    protected boolean enabled;
    protected String factoryClassName;
    protected Properties properties = new Properties();

    /**
     * Returns the factory class name.
     */
    public String getFactoryClassName() {
        return factoryClassName;
    }

    /**
     * Sets the factory class name.
     */
    public T setFactoryClassName(@Nonnull String factoryClassName) {
        this.factoryClassName =  checkHasText(factoryClassName, "The factoryClassName cannot be null!");
        return self();
    }

    /**
     * Returns if this configuration is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables and disables this configuration.
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public T setEnabled(boolean enabled) {
        this.enabled = enabled;
        return self();
    }

    /**
     * Sets a single property.
     *
     * @param name  the name of the property to set
     * @param value the value of the property to set
     * @return the updated config object (self)
     * @throws NullPointerException if name or value is {@code null}
     */
    public T setProperty(String name, String value) {
        properties.put(name, value);
        return self();
    }

    /**
     * Gets a property.
     *
     * @param name the name of the property to get
     * @return the value of the property, null if not found
     * @throws NullPointerException if name is {@code null}
     */
    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    /**
     * Gets all properties.
     *
     * @return the properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties.
     *
     * @param properties the properties to set
     * @return the updated config object (self)
     * @throws IllegalArgumentException if properties is {@code null}
     */
    public T setProperties(@Nonnull Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties can't be null");
        }
        this.properties = properties;
        return self();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + "factoryClassName='" + factoryClassName + '\''
                + ", enabled=" + enabled
                + ", properties=" + properties
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractFactoryWithPropertiesConfig<?> otherConfig = (AbstractFactoryWithPropertiesConfig<?>) o;

        return Objects.equals(enabled, otherConfig.enabled)
            && Objects.equals(properties, otherConfig.properties)
            && Objects.equals(factoryClassName, otherConfig.factoryClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, factoryClassName, properties);
    }

    protected abstract T self();

}

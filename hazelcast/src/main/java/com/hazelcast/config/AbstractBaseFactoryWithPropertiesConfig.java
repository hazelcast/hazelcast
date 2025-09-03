/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkHasText;

/**
 * Configuration base for config types with a factory class and its properties.
 *
 * @param <T> final child type
 */
public abstract class AbstractBaseFactoryWithPropertiesConfig<T extends AbstractBaseFactoryWithPropertiesConfig<T>> {
    @Nullable
    protected String factoryClassName;
    protected Properties properties = new Properties();

    /**
     * Returns the factory class name.
     */
    @Nullable
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
     * Sets a single property.
     *
     * @param name  the name of the property to set
     * @param value the value of the property to set
     * @return the updated config object (self)
     * @throws NullPointerException if name or value is {@code null}
     */
    public T setProperty(String name, String value) {
        properties.setProperty(name, value);
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

        AbstractBaseFactoryWithPropertiesConfig<?> otherConfig = (AbstractBaseFactoryWithPropertiesConfig<?>) o;

        return Objects.equals(properties, otherConfig.properties)
            && Objects.equals(factoryClassName, otherConfig.factoryClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(factoryClassName, properties);
    }

    protected abstract T self();

}

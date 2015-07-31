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

import com.hazelcast.nio.serialization.Serializer;

/**
 * Contains the serialization configuration for a particular class.
 */
public class SerializerConfig {

    private String className;

    private Serializer implementation;

    private Class typeClass;

    private String typeClassName;

    public SerializerConfig() {
        super();
    }

    /**
     * Returns the class name of the serializer implementation.
     *
     * @return the class name of serializer implementation
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the class of the serializer implementation.
     *
     * @param clazz the set class of the serializer implementation
     * @return SerializerConfig
     */
    public SerializerConfig setClass(final Class<? extends Serializer> clazz) {
        String className = clazz == null ? null : clazz.getName();
        return setClassName(className);
    }

    /**
     * Sets the class name of the serializer implementation.
     *
     * @param className the class name of the serializer implementation
     * @return SerializationConfig
     */
    public SerializerConfig setClassName(final String className) {
        this.className = className;
        return this;
    }

    /**
     * Returns the implementation of the serializer class.
     *
     * @return the implementation of the serializer class
     * @see {@link com.hazelcast.config.SerializerConfig#setImplementation(com.hazelcast.nio.serialization.Serializer)}
     */
    public Serializer getImplementation() {
        return implementation;
    }

    /**
     * Sets the serializer implementation instance.
     * <br/>
     * Serializer must be instance of either {@link com.hazelcast.nio.serialization.StreamSerializer}
     * or {@link com.hazelcast.nio.serialization.ByteArraySerializer}.
     *
     * @param implementation the serializer instance
     * @return SerializerConfig
     */
    public SerializerConfig setImplementation(final Serializer implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * Gets the type of the class that will be serialized via this implementation.
     *
     * @return typeClass type of the class that will be serialized via this implementation
     * @see {@link com.hazelcast.config.SerializerConfig#setTypeClass(Class)}
     */
    public Class getTypeClass() {
        return typeClass;
    }

    /**
     * Sets the type of the class that will be serialized via this implementation.
     *
     * @param typeClass type of the class that will be serialized via this implementation
     * @return SerializerConfig
     */
    public SerializerConfig setTypeClass(final Class typeClass) {
        this.typeClass = typeClass;
        return this;
    }

    /**
     * Gets the name of the class that will be serialized via this implementation.
     *
     * @return typeClassName name of the class that will be serialized via this implementation
     * @see {@link com.hazelcast.config.SerializerConfig#setTypeClassName(String)}
     */
    public String getTypeClassName() {
        return typeClassName;
    }

    /**
     * This method is called only if typeClass is not set. If type class is not set, class will try to be loaded
     * from class loader via given className using this method.
     *
     * @param typeClassName name of the class that will be serialized via this implementation
     * @return SerializerConfig
     */
    public SerializerConfig setTypeClassName(final String typeClassName) {
        this.typeClassName = typeClassName;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SerializerConfig{");
        sb.append("className='").append(className).append('\'');
        sb.append(", implementation=").append(implementation);
        sb.append(", typeClass=").append(typeClass);
        sb.append(", typeClassName='").append(typeClassName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

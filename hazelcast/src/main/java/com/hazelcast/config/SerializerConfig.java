/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Objects;

/**
 * Contains the serialization configuration for a particular class.
 */
public class SerializerConfig {

    private String className;

    private Serializer implementation;

    private Class typeClass;

    private String typeClassName;

    public SerializerConfig() {
    }

    public SerializerConfig(SerializerConfig serializerConfig) {
        className = serializerConfig.className;
        implementation = serializerConfig.implementation;
        typeClass = serializerConfig.typeClass;
        typeClassName = serializerConfig.typeClassName;
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
        if (className != null) {
            setImplementation(null);
        }
        this.className = className;
        return this;
    }

    /**
     * Returns the implementation of the serializer class.
     *
     * @return the implementation of the serializer class
     * @see com.hazelcast.config.SerializerConfig#setImplementation(com.hazelcast.nio.serialization.Serializer)
     */
    public Serializer getImplementation() {
        return implementation;
    }

    /**
     * Sets the serializer implementation instance.
     * <br>
     * Serializer must be instance of either {@link com.hazelcast.nio.serialization.StreamSerializer}
     * or {@link com.hazelcast.nio.serialization.ByteArraySerializer}.
     *
     * @param implementation the serializer instance
     * @return SerializerConfig
     */
    public SerializerConfig setImplementation(final Serializer implementation) {
        if (implementation != null) {
            setClassName(null);
        }
        this.implementation = implementation;
        return this;
    }

    /**
     * Gets the type of the class that will be serialized via this implementation.
     *
     * @return typeClass type of the class that will be serialized via this implementation
     * @see com.hazelcast.config.SerializerConfig#setTypeClass(Class)
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
        if (typeClass != null) {
            setTypeClassName(null);
        }
        this.typeClass = typeClass;
        return this;
    }

    /**
     * Gets the name of the class that will be serialized via this implementation.
     *
     * @return typeClassName name of the class that will be serialized via this implementation
     * @see com.hazelcast.config.SerializerConfig#setTypeClassName(String)
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
        if (typeClassName != null) {
            setTypeClass(null);
        }
        this.typeClassName = typeClassName;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof SerializerConfig)) {
            return false;
        }

        SerializerConfig that = (SerializerConfig) o;

        return Objects.equals(implementationNameInternal(), that.implementationNameInternal())
            && Objects.equals(typeClassNameInternal(), that.typeClassNameInternal());
    }

    private String implementationNameInternal() {
        if (implementation != null) {
            return implementation.getClass().getName();
        }
        return className;
    }

    private String typeClassNameInternal() {
        if (typeClass != null) {
            return typeClass.getName();
        }
        return typeClassName;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(typeClassNameInternal(), implementationNameInternal());
    }

    @Override
    public String toString() {
        return "SerializerConfig{"
                + "className='" + className + '\''
                + ", implementation=" + implementation
                + ", typeClass=" + typeClass
                + ", typeClassName='" + typeClassName + '\''
                + '}';
    }
}

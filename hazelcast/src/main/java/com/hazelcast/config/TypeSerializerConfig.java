/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.TypeSerializer;

public class TypeSerializerConfig {

    private String className;

    private TypeSerializer implementation;

    private Class typeClass;

    private String typeClassName;

    public TypeSerializerConfig() {
        super();
    }

    public String getClassName() {
        return className;
    }

    public TypeSerializerConfig setClassName(final String className) {
        this.className = className;
        return this;
    }

    public TypeSerializer getImplementation() {
        return implementation;
    }

    public TypeSerializerConfig setImplementation(final TypeSerializer implementation) {
        this.implementation = implementation;
        return this;
    }

    public Class getTypeClass() {
        return typeClass;
    }

    public TypeSerializerConfig setTypeClass(final Class typeClass) {
        this.typeClass = typeClass;
        return this;
    }

    public String getTypeClassName() {
        return typeClassName;
    }

    public TypeSerializerConfig setTypeClassName(final String typeClassName) {
        this.typeClassName = typeClassName;
        return this;
    }
}

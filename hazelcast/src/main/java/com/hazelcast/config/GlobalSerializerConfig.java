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

import com.hazelcast.nio.serialization.Serializer;
/**
 * Contains the configuration for global serializer.
 */
public class GlobalSerializerConfig {

    private String className;

    private Serializer implementation;

    public GlobalSerializerConfig() {
        super();
    }

    public String getClassName() {
        return className;
    }

    public GlobalSerializerConfig setClassName(final String className) {
        this.className = className;
        return this;
    }

    public Serializer getImplementation() {
        return implementation;
    }

    public GlobalSerializerConfig setImplementation(Serializer implementation) {
        this.implementation = implementation;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GlobalSerializerConfig{");
        sb.append("className='").append(className).append('\'');
        sb.append(", implementation=").append(implementation);
        sb.append('}');
        return sb.toString();
    }
}

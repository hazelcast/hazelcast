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

package com.hazelcast.config;

import com.hazelcast.nio.serialization.Serializer;
/**
 * Contains the configuration for global serializer.
 */
public class GlobalSerializerConfig {

    private String className;

    private Serializer implementation;

    private boolean overrideJavaSerialization;

    public GlobalSerializerConfig() {
    }

    public GlobalSerializerConfig(GlobalSerializerConfig globalSerializerConfig) {
        className = globalSerializerConfig.className;
        implementation = globalSerializerConfig.implementation;
        overrideJavaSerialization = globalSerializerConfig.overrideJavaSerialization;
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

    public boolean isOverrideJavaSerialization() {
        return overrideJavaSerialization;
    }

    public GlobalSerializerConfig setOverrideJavaSerialization(boolean overrideJavaSerialization) {
        this.overrideJavaSerialization = overrideJavaSerialization;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof GlobalSerializerConfig)) {
            return false;
        }

        GlobalSerializerConfig that = (GlobalSerializerConfig) o;

        if (overrideJavaSerialization != that.overrideJavaSerialization) {
            return false;
        }
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        return implementation != null ? implementation.equals(that.implementation) : that.implementation == null;
    }

    @Override
    public final int hashCode() {
        int result = className != null ? className.hashCode() : 0;
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        result = 31 * result + (overrideJavaSerialization ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GlobalSerializerConfig{"
                + "className='" + className + '\''
                + ", implementation=" + implementation
                + ", overrideJavaSerialization=" + overrideJavaSerialization
                + '}';
    }
}

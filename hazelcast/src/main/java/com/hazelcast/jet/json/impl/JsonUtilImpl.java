/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.json.impl;

import com.hazelcast.config.ClassFilter;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.json.JsonUtil;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public final class JsonUtilImpl {
    /**
     * If the default classes should not be added to blocklist for JSON deserialization.
     * Disabling defaults is insecure.
     */
    static final String JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY = "hazelcast.jet.json.blocklist.defaultsDisabled";
    static final ClassFilter JSON_DEFAULT_BLOCKLIST = createJsonDefaultBlocklist();
    static final String JSON_DEFAULT_BLOCKLIST_HINT = String.format(
            "%nThis restriction can be disabled by setting property `%s` to true", JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY);

    private JsonUtilImpl() {
    }

    /**
     * Secure parser of JSON to given type.
     * Rejects dangerous types.
     */
    @Nonnull
    public static <T> FunctionEx<String, T> toBean(@Nonnull Class<T> type) {
        return new JsonUtilImpl.BeanFromJsonFn<>(type);
    }

    /**
     * Secure parser of JSON to given type.
     * Rejects dangerous types.
     */
    @Nonnull
    public static <K, T> BiFunctionEx<K, String, T> toBeanBiFn(@Nonnull Class<T> type) {
        return new BeanFromJsonBiFn<>(type);
    }

    abstract static class JsonFilteredFunction<T> implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        protected final Class<T> type;

        protected JsonFilteredFunction(@Nonnull Class<T> type) {
            // validate type once to avoid overhead during processing
            ensureTypeIsAllowed(type);
            this.type = type;
        }

        @Serial
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            // validate type once to avoid overhead during processing
            ensureTypeIsAllowed(type);
        }

        private static void ensureTypeIsAllowed(@Nonnull Class<?> type) {
            Objects.requireNonNull(type, "type must not be null");
            if (!Boolean.getBoolean(JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY)) {
                if (isBlockedByDefaultBlocklist(getElementType(type))) {
                    throw new IllegalArgumentException(String.format("Class %s cannot be deserialized using JSON.%s",
                            type.getName(), JSON_DEFAULT_BLOCKLIST_HINT));
                }
            }
        }
    }

    private static ClassFilter createJsonDefaultBlocklist() {
        ClassFilter blockList = new ClassFilter();
        blockList.addPrefixes(
                "com.hazelcast.shaded",
                "com.hazelcast.internal",
                "com.hazelcast.map.impl",
                "org.springframework.context",
                "org.springframework.beans"
        );
        blockList.addPackages(
                "com.hazelcast.config",
                "com.hazelcast.client.config"
        );
        return blockList;
    }

    static boolean isBlockedByDefaultBlocklist(Class<?> type) {
        return DataSource.class.isAssignableFrom(type)
                || JSON_DEFAULT_BLOCKLIST.isListed(type.getName());
    }

    /**
     * Gets element type of potentially multidimensional array if this is array, otherwise returns the type.
     * @param type type than can be an array
     * @return element type of array or the type itself if it is not array
     */
    @Nonnull
    public static Class<?> getElementType(@Nonnull Class<?> type) {
        while (type.isArray()) {
            type = type.getComponentType();
        }
        return type;
    }

    private static class BeanFromJsonFn<T> extends JsonFilteredFunction<T> implements FunctionEx<String, T> {

        @Serial
        private static final long serialVersionUID = 1L;

        BeanFromJsonFn(@Nonnull Class<T> type) {
            super(type);
        }

        @Override
        public T applyEx(String line) throws Exception {
            return JsonUtil.beanFrom(line, type);
        }
    }

    private static class BeanFromJsonBiFn<K, T> extends JsonFilteredFunction<T> implements BiFunctionEx<K, String, T> {

        @Serial
        private static final long serialVersionUID = 1L;

        BeanFromJsonBiFn(@Nonnull Class<T> type) {
            super(type);
        }

        @Override
        public T applyEx(K ignored, String line) throws Exception {
            return JsonUtil.beanFrom(line, type);
        }
    }
}

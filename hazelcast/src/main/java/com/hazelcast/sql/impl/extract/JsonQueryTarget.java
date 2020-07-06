/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeMismatchException;

import java.util.Set;

public class JsonQueryTarget implements QueryTarget {

    private final Set<String> staticallyTypedPaths;

    private final Extractors extractors;
    private final boolean key;

    private Object target;

    public JsonQueryTarget(Set<String> staticallyTypedPaths, Extractors extractors, boolean key) {
        this.staticallyTypedPaths = staticallyTypedPaths;

        this.extractors = extractors;
        this.key = key;
    }

    @Override
    public void setTarget(Object target) {
        if ((target instanceof Data && !((Data) target).isJson())
                || (!(target instanceof Data) && !(target instanceof HazelcastJsonValue))) {
            throw new IllegalArgumentException("Not a Json object");
        }

        this.target = target;
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        if (path == null) {
            return createObjectExtractor(type);
        } else if (staticallyTypedPaths.contains(path)) {
            return createFieldExtractor(path, type);
        } else {
            return createDynamicFieldExtractor(path, type);
        }
    }

    private QueryExtractor createObjectExtractor(QueryDataType type) {
        return () -> {
            try {
                return type.normalize(target);
            } catch (QueryDataTypeMismatchException e) {
                throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value")
                        + " because of type mismatch [expectedClass=" + e.getExpectedClass().getName()
                        + ", actualClass=" + e.getActualClass().getName() + "]"
                );
            } catch (Exception e) {
                throw QueryException.dataException(
                        "Failed to extract map entry " + (key ? "key" : "value") + ": " + e.getMessage(), e
                );
            }
        };
    }

    private QueryExtractor createFieldExtractor(String path, QueryDataType type) {
        return () -> {
            try {
                return type.normalize(extractors.extract(target, path, null, true));
            } catch (QueryDataTypeMismatchException e) {
                throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value") + " field \""
                        + path + "\" because of type mismatch [expectedClass=" + e.getExpectedClass().getName()
                        + ", actualClass=" + e.getActualClass().getName() + ']');
            } catch (Exception e) {
                throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value") + " field \""
                        + path + "\": " + e.getMessage(), e);
            }
        };
    }

    private QueryExtractor createDynamicFieldExtractor(String path, QueryDataType type) {
        return () -> {
            try {
                return type.convert(extractors.extract(target, path, null));
            } catch (Exception e) {
                throw QueryException.dataException(
                        "Failed to extract map entry " + (key ? "key" : "value") + " field \"" + path + "\": " + e.getMessage(), e
                );
            }
        };
    }
}

/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeMismatchException;

import javax.annotation.concurrent.NotThreadSafe;

// remove in favor of IMDG implementation when JSON is supported
@NotThreadSafe
class HazelcastJsonQueryTarget implements QueryTarget {

    private final SerializationService serializationService;
    private final Extractors extractors;
    private final boolean key;

    private Object target;

    HazelcastJsonQueryTarget(SerializationService serializationService, Extractors extractors, boolean key) {
        this.serializationService = serializationService;
        this.extractors = extractors;
        this.key = key;
    }

    @Override
    public void setTarget(Object target, Data targetData) {
        if (target == null && targetData != null) {
            this.target = targetData;
        } else {
            this.target = target;
        }
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        return path == null ? createExtractor(type) : createFieldExtractor(path, type);
    }

    private QueryExtractor createExtractor(QueryDataType type) {
        return () -> {
            try {
                Object value = serializationService.toObject(target);
                return type.convert(value);
            } catch (QueryDataTypeMismatchException e) {
                throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value")
                        + " because of type mismatch [expectedClass=" + e.getExpectedClass().getName()
                        + ", actualClass=" + e.getActualClass().getName() + ']');
            } catch (Exception e) {
                throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value") + ": "
                        + e.getMessage(), e);
            }
        };
    }

    private QueryExtractor createFieldExtractor(String path, QueryDataType type) {
        return () -> {
            try {
                Object value = extractors.extract(target, path, null, false);
                return type.convert(value);
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
}

/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeMismatchException;

/**
 * An extractor that uses {@link com.hazelcast.query.impl.getters.Extractors} for field retrieval.
 */
public class GenericFieldExtractor extends AbstractGenericExtractor {

    private final Extractors extractors;
    private final String path;

    public GenericFieldExtractor(
        boolean key,
        GenericTargetAccessor targetAccessor,
        QueryDataType type,
        Extractors extractors,
        String path
    ) {
        super(key, targetAccessor, type);

        this.extractors = extractors;
        this.path = path;
    }

    private Object getInternal(boolean useLazyDeserialization) {
        try {
            Object target = targetAccessor.getTargetForFieldAccess();
            Object value = extractors.extract(target, path, useLazyDeserialization, false);
            return type.normalize(value);
        } catch (QueryDataTypeMismatchException e) {
            throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value") + " field \""
                + path + "\" because of type mismatch [expectedClass=" + e.getExpectedClass().getName()
                + ", actualClass=" + e.getActualClass().getName() + ']');
        } catch (Exception e) {
            throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value") + " field \""
                + path + "\": " + e.getMessage(), e);
        }
    }

    @Override
    public Object get() {
        return getInternal(false);
    }

    @Override
    public Object get(boolean useLazyDeserialization) {
        return getInternal(useLazyDeserialization);
    }
}

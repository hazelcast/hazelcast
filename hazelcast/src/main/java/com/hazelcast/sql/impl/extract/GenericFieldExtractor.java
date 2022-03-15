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

    @Override
    public Object get() {
        try {
            Object target = targetAccessor.getTargetForFieldAccess();
            Object value = extractors.extract(target, path, null, false);
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
}

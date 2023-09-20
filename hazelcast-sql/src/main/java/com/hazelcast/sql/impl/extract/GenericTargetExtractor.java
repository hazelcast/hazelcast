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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeMismatchException;

/**
 * An extractor that returns the target itself.
 */
public class GenericTargetExtractor extends AbstractGenericExtractor {
    public GenericTargetExtractor(boolean key, GenericTargetAccessor targetAccessor, QueryDataType type) {
        super(key, targetAccessor, type);
    }

    @Override
    public Object get() {
        try {
            return targetAccessor.getTargetForDirectAccess(type);
        } catch (QueryDataTypeMismatchException e) {
            throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value")
                + " because of type mismatch [expectedClass=" + e.getExpectedClass().getName()
                + ", actualClass=" + e.getActualClass().getName() + ']');
        } catch (Exception e) {
            throw QueryException.dataException("Failed to extract map entry " + (key ? "key" : "value") + ": "
                + e.getMessage(), e);
        }
    }
}

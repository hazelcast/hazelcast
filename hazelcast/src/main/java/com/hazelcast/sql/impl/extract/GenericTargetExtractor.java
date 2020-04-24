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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

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
            return type.convert(getTarget());
        } catch (Exception e) {
            throw QueryException.dataException("Cannot convert " + (key ? "key" : "value") + " to " + type + ": "
                + e.getMessage(), e);
        }
    }
}

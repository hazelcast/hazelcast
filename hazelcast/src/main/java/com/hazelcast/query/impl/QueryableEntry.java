/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.QueryException;

import java.util.Map;

import static com.hazelcast.query.impl.TypeConverters.IDENTITY_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;

/**
 * This interface contains methods related to Queryable Entry which means searched an indexed by sql query or predicate .
 */
public abstract class QueryableEntry implements Map.Entry {

    protected SerializationService serializationService;

    protected Extractors extractors;

    public Object getAttributeValue(String attributeName) throws QueryException {
        return ExtractionEngine.extractAttributeValue(extractors, serializationService, attributeName, this);
    }

    public AttributeType getAttributeType(String attributeName) throws QueryException {
        return ExtractionEngine.extractAttributeType(extractors, serializationService, attributeName, this, null);
    }

    TypeConverter getConverter(String attributeName) {
        Object attribute = getAttributeValue(attributeName);
        if (attribute == null) {
            return NULL_CONVERTER;
        } else {
            AttributeType attributeType = ExtractionEngine.extractAttributeType(extractors, serializationService,
                    attributeName, this, attribute);
            return attributeType == null ? IDENTITY_CONVERTER : attributeType.getConverter();
        }
    }

    public abstract Object getValue();

    public abstract Object getKey();

    public abstract Data getKeyData();

    public abstract Data getValueData();

    protected abstract Object getTargetObject(boolean key);

}

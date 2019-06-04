/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Metadata;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.AttributeOrigin;

import java.util.Map;

/**
 * This abstract class contains methods related to Queryable Entry, which means searched an indexed by SQL query or predicate.
 * <p>
 * If the object, which is used as the extraction target, is not of Data or Portable type the serializationService
 * will not be touched at all.
 */
public abstract class QueryableEntry<K, V> implements Map.Entry<K, V> {

    protected InternalSerializationService serializationService;
    protected Extractors extractors;

    private Metadata metadata;

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Extractors getExtractors() {
        return extractors;
    }

    public abstract V getValue();

    public abstract K getKey();

    public abstract Data getKeyData();

    public abstract Data getValueData();

    protected Object getTargetObject(AttributeOrigin attributeOrigin) {
        switch (attributeOrigin) {
            case KEY:
            case WITHIN_KEY:
                return getKey();
            case VALUE:
            case WITHIN_VALUE:
                return getValue();
            default:
                throw new IllegalArgumentException();
        }
    }
}

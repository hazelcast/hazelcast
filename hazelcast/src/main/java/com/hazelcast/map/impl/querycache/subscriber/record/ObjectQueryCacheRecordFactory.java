/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Factory for {@link ObjectQueryCacheRecord}.
 *
 * @see ObjectQueryCacheRecord
 */
public class ObjectQueryCacheRecordFactory implements QueryCacheRecordFactory {

    private final SerializationService serializationService;

    public ObjectQueryCacheRecordFactory(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public QueryCacheRecord createRecord(Data valueData) {
        return new ObjectQueryCacheRecord(valueData, serializationService);
    }

    @Override
    public boolean isEquals(Object value1, Object value2) {
        Object v1 = value1 instanceof Data ? serializationService.toObject(value1) : value1;
        Object v2 = value2 instanceof Data ? serializationService.toObject(value2) : value2;
        if (v1 == null && v2 == null) {
            return true;
        }
        if (v1 == null) {
            return false;
        }
        if (v2 == null) {
            return false;
        }
        return v1.equals(v2);
    }
}

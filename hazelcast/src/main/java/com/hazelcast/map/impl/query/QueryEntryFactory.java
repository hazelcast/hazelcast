/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;

public final class QueryEntryFactory {
    private final CacheDeserializedValues cacheDeserializedValues;

    public QueryEntryFactory(CacheDeserializedValues cacheDeserializedValues) {
        this.cacheDeserializedValues = cacheDeserializedValues;
    }

    public QueryableEntry newEntry(InternalSerializationService serializationService,
                                   Data key, Object value, Extractors extractors) {
        switch (cacheDeserializedValues) {
            case NEVER:
                return new QueryEntry(serializationService, key, value, extractors);
            default:
                return new CachedQueryEntry(serializationService, key, value, extractors);
        }
    }
}

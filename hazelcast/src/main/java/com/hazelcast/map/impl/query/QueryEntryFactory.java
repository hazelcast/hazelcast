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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;

public final class QueryEntryFactory {

    private final CacheDeserializedValues cacheDeserializedValues;
    private final InternalSerializationService serializationService;
    private final Extractors extractors;

    public QueryEntryFactory(CacheDeserializedValues cacheDeserializedValues,
                             InternalSerializationService serializationService,
                             Extractors extractors) {
        this.cacheDeserializedValues = cacheDeserializedValues;
        this.serializationService = serializationService;
        this.extractors = extractors;
    }

    public QueryableEntry newEntry(Data key, Object value) {
        switch (cacheDeserializedValues) {
            case NEVER:
                return new QueryEntry(serializationService, key, value, extractors);
            default:
                return new CachedQueryEntry(serializationService, key, value, extractors);
        }
    }
}

/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.SampleableConcurrentHashMap;

/**
 * An extended {@link SampleableConcurrentHashMap} with {@link com.hazelcast.core.IMap} specifics.
 *
 * @param <R> Type of records in this CHM
 */
public class StorageSCHM<R extends Record> extends SampleableConcurrentHashMap<Data, R> {

    private static final int DEFAULT_INITIAL_CAPACITY = 256;

    private final SerializationService serializationService;

    public StorageSCHM(SerializationService serializationService) {
        super(DEFAULT_INITIAL_CAPACITY);

        this.serializationService = serializationService;
    }

    @Override
    protected <E extends SamplingEntry> E createSamplingEntry(Data key, R record) {
        return (E) new LazyEntryViewFromRecord<R>(record, serializationService);
    }
}

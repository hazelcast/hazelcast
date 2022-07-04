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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.SampleableConcurrentHashMap;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;

/**
 * An extended {@link SampleableConcurrentHashMap} with {@link IMap} specifics.
 *
 * @param <R> Type of records in this CHM
 */
@SerializableByConvention
public class StorageSCHM<R extends Record> extends SampleableConcurrentHashMap<Data, R> {

    private static final long serialVersionUID = -1133966339806826032L;
    private static final int DEFAULT_INITIAL_CAPACITY = 256;

    private final transient SerializationService serializationService;
    private final transient ExpirySystem expirySystem;

    public StorageSCHM(SerializationService serializationService, ExpirySystem expirySystem) {
        super(DEFAULT_INITIAL_CAPACITY);

        this.serializationService = serializationService;
        this.expirySystem = expirySystem;
    }

    @Override
    protected <E extends SamplingEntry> E createSamplingEntry(Data key, R record) {
        return (E) new LazyEvictableEntryView<>(key, record,
                expirySystem.getExpiryMetadata(key), serializationService);
    }
}

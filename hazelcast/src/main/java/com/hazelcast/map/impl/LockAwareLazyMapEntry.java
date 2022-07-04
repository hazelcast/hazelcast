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

package com.hazelcast.map.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.LockAware;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Specialization of the LazyMapEntry that is LockAware. Used in
 * EntryProcessor. If serialized the locked property will be nullified,
 * since it's volatile and valid only when on partition-thread.
 */
public class LockAwareLazyMapEntry extends LazyMapEntry implements LockAware {

    private static final long serialVersionUID = 0L;

    // not to be serialized, if serialized should return null
    private transient Boolean locked;

    public LockAwareLazyMapEntry() {
    }

    public LockAwareLazyMapEntry(Data key, Object value, InternalSerializationService serializationService,
                                 Extractors extractors, Boolean locked) {
        super(key, value, serializationService, extractors);
        this.locked = locked;
    }

    public LockAwareLazyMapEntry init(InternalSerializationService serializationService,
                                      Data key, Object value, Extractors extractors, Boolean locked, long ttl) {
        super.init(serializationService, key, value, extractors, ttl);
        this.locked = locked;
        return this;
    }

    @Override
    public Boolean isLocked() {
        return locked;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.LOCK_AWARE_LAZY_MAP_ENTRY;
    }

}

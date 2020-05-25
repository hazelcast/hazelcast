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

package com.hazelcast.internal.compatibility.cache;

import com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.CACHE_DS_FACTORY;
import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.CACHE_DS_FACTORY_ID;


/**
 * Data serializer hook containing (de)serialization information for
 * JCache-related classes used when communicating with 3.x members over WAN.
 */
public final class CompatibilityCacheDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = CompatibilityFactoryIdHelper.getFactoryId(
            CACHE_DS_FACTORY, CACHE_DS_FACTORY_ID);

    public static final short DEFAULT_CACHE_ENTRY_VIEW = 44;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case DEFAULT_CACHE_ENTRY_VIEW:
                    return new CompatibilityWanCacheEntryView();
                default:
                    return null;
            }
        };
    }
}

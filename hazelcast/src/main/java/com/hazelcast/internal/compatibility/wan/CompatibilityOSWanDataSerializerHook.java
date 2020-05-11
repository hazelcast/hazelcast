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

package com.hazelcast.internal.compatibility.wan;

import com.hazelcast.internal.compatibility.cache.CompatibilityWanCacheEntryView;
import com.hazelcast.internal.compatibility.map.CompatibilityWanMapEntryView;
import com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.WAN_REPLICATION_DS_FACTORY;
import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.WAN_REPLICATION_DS_FACTORY_ID;


/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 4.x members over WAN. Compatibility WAN replication is only supported
 * for EE so support for OS classes here is lacking.
 */
@SuppressWarnings("unused")
public class CompatibilityOSWanDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = CompatibilityFactoryIdHelper.getFactoryId(
            WAN_REPLICATION_DS_FACTORY, WAN_REPLICATION_DS_FACTORY_ID);

    public static final int MAP_REPLICATION_UPDATE = 0;
    public static final int MAP_REPLICATION_REMOVE = 1;
    public static final int WAN_MAP_ENTRY_VIEW = 2;
    public static final int WAN_CACHE_ENTRY_VIEW = 3;
    public static final int WAN_EVENT_CONTAINER_REPLICATION_OPERATION = 4;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case WAN_MAP_ENTRY_VIEW:
                        return new CompatibilityWanMapEntryView<>();
                    case WAN_CACHE_ENTRY_VIEW:
                        return new CompatibilityWanCacheEntryView<>();
                    default:
                        throw new IllegalArgumentException("Unknown type-id: " + typeId);
                }
            }
        };
    }
}

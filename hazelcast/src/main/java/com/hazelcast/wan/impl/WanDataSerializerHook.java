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

package com.hazelcast.wan.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.wan.MapReplicationRemove;
import com.hazelcast.map.impl.wan.MapReplicationUpdate;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.WAN_REPLICATION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.WAN_REPLICATION_DS_FACTORY_ID;

/**
 * {@link com.hazelcast.internal.serialization.DataSerializerHook} implementation for Wan Replication classes
 */
public class WanDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(WAN_REPLICATION_DS_FACTORY, WAN_REPLICATION_DS_FACTORY_ID);

    public static final int MAP_REPLICATION_UPDATE = 0;
    public static final int MAP_REPLICATION_REMOVE = 1;
    public static final int WAN_MAP_ENTRY_VIEW = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case MAP_REPLICATION_UPDATE:
                    return new MapReplicationUpdate();
                case MAP_REPLICATION_REMOVE:
                    return new MapReplicationRemove();
                case WAN_MAP_ENTRY_VIEW:
                    return new WanMapEntryView<>();
                default:
                    throw new IllegalArgumentException("Unknown type-id: " + typeId);
            }
        };
    }
}

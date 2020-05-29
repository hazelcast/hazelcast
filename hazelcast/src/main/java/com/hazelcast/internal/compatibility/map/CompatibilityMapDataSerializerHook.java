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

package com.hazelcast.internal.compatibility.map;

import com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.MAP_DS_FACTORY;
import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.MAP_DS_FACTORY_ID;


/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 4.x members over WAN.
 */
public final class CompatibilityMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = CompatibilityFactoryIdHelper.getFactoryId(MAP_DS_FACTORY, MAP_DS_FACTORY_ID);

    public static final int ENTRY_VIEW = 8;
    public static final int MERKLE_TREE_NODE_ENTRIES = 139;

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
                    case ENTRY_VIEW:
                        return new CompatibilityWanMapEntryView<Object, Object>();
                    case MERKLE_TREE_NODE_ENTRIES:
                        return new CompatibilityMerkleTreeNodeEntries();
                    default:
                        return null;
                }
            }
        };
    }
}

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

import com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;

/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 3.x members over WAN. Compatibility WAN replication is only supported
 * for EE so support for OS classes here is lacking.
 */
public class CompatibilityOSWanDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = CompatibilityFactoryIdHelper.getFactoryId(
            CompatibilityFactoryIdHelper.WAN_REPLICATION_DS_FACTORY,
            CompatibilityFactoryIdHelper.WAN_REPLICATION_DS_FACTORY_ID);

    public static final int WAN_REPLICATION_EVENT = 0;
    public static final int MAP_REPLICATION_UPDATE = 1;
    public static final int MAP_REPLICATION_REMOVE = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case WAN_REPLICATION_EVENT:
                    return new CompatibilityWanReplicationEvent();
                default:
                    return null;
            }
        };
    }
}

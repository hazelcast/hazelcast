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

package com.hazelcast.internal.crdt;

import com.hazelcast.internal.crdt.pncounter.PNCounterImpl;
import com.hazelcast.internal.crdt.pncounter.PNCounterReplicationOperation;
import com.hazelcast.internal.crdt.pncounter.operations.AddOperation;
import com.hazelcast.internal.crdt.pncounter.operations.CRDTTimestampedLong;
import com.hazelcast.internal.crdt.pncounter.operations.GetOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PN_COUNTER_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PN_COUNTER_DS_FACTORY_ID;

/**
 * Serialization constants for all CRDT related objects
 */
public final class CRDTDataSerializerHook implements DataSerializerHook {
    /** The CRDT (de)serializer factory ID */
    public static final int F_ID = FactoryIdHelper.getFactoryId(PN_COUNTER_DS_FACTORY, PN_COUNTER_DS_FACTORY_ID);
    /** The (de)serialization constant for a PNCounter replication operation */
    public static final int PN_COUNTER_REPLICATION = 1;
    /** The (de)serialization constant for a PNCounter implementation */
    public static final int PN_COUNTER = 2;
    /** The constant for the PNCounter {@link AddOperation} */
    public static final int PN_COUNTER_ADD_OPERATION = 3;
    /** The constant for the PNCounter {@link GetOperation} */
    public static final int PN_COUNTER_GET_OPERATION = 4;
    /** The constant for the PNCounter {@link CRDTTimestampedLong} */
    public static final int CRDT_TIMESTAMPED_LONG = 5;

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
                    case PN_COUNTER_REPLICATION:
                        return new PNCounterReplicationOperation();
                    case PN_COUNTER:
                        return new PNCounterImpl();
                    case PN_COUNTER_ADD_OPERATION:
                        return new AddOperation();
                    case PN_COUNTER_GET_OPERATION:
                        return new GetOperation();
                    case CRDT_TIMESTAMPED_LONG:
                        return new CRDTTimestampedLong();
                    default:
                        return null;
                }
            }
        };
    }
}

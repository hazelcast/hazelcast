/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapCompareAndSetOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapDeleteOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapGetOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapPutIfAbsentOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapPutOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapRemoveOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapSetOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

/**
 * Registers the open source CP Map types.
 */
public class CPMapDataSerializerHook implements DataSerializerHook {
    public static final int PUT_OP = 1;
    public static final int SET_OP = 2;
    public static final int REMOVE_OP = 3;
    public static final int DELETE_OP = 4;
    public static final int CAS_OP = 5;
    public static final int GET_OP = 6;
    public static final int PUT_IF_ABSENT_OP = 7;

    /**
     * LEN = 18 because we merge an extension constructor into
     * this factory to keep RU compatibility between versions.
     */
    public static final int LEN = 18;

    private static final String RAFT_CPMAP_DS_FACTORY = "hazelcast.serialization.ds.raft.cpmap";
    private static final int RAFT_CPMAP_DS_FACTORY_DEFAULT_ID = -1079;
    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_CPMAP_DS_FACTORY, RAFT_CPMAP_DS_FACTORY_DEFAULT_ID);

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[PUT_OP] = CPMapPutOp::new;
        constructors[SET_OP] = CPMapSetOp::new;
        constructors[REMOVE_OP] = CPMapRemoveOp::new;
        constructors[DELETE_OP] = CPMapDeleteOp::new;
        constructors[CAS_OP] = CPMapCompareAndSetOp::new;
        constructors[GET_OP] = CPMapGetOp::new;
        constructors[PUT_IF_ABSENT_OP] = CPMapPutIfAbsentOp::new;
        // 12..17 stay null here: they are owned by the enterprise hook,
        // which merges them into this factory instance in afterFactoriesCreated().

        return new ArrayDataSerializableFactory(constructors);
    }
}

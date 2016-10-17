/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.internal.management.operation.GetMapConfigOperation;
import com.hazelcast.internal.management.operation.ScriptExecutorOperation;
import com.hazelcast.internal.management.operation.ThreadDumpOperation;
import com.hazelcast.internal.management.operation.UpdateManagementCenterUrlOperation;
import com.hazelcast.internal.management.operation.UpdateMapConfigOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MANAGEMENT_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MANAGEMENT_DS_FACTORY_ID;

public class ManagementDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(MANAGEMENT_DS_FACTORY, MANAGEMENT_DS_FACTORY_ID);

    public static final int CHANGE_WAN = 0;
    public static final int GET_MAP_CONFIG = 1;
    public static final int SCRIPT_EXECUTOR = 2;
    public static final int THREAD_DUMP = 3;
    public static final int UPDATE_MANAGEMENT_CENTER_URL = 4;
    public static final int UPDATE_MAP_CONFIG = 5;

    private static final int LEN = UPDATE_MAP_CONFIG + 1;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors
                = new ConstructorFunction[LEN];

        constructors[CHANGE_WAN] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ChangeWanStateOperation();
            }
        };
        constructors[GET_MAP_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GetMapConfigOperation();
            }
        };
        constructors[SCRIPT_EXECUTOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ScriptExecutorOperation();
            }
        };
        constructors[THREAD_DUMP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ThreadDumpOperation();
            }
        };
        constructors[UPDATE_MANAGEMENT_CENTER_URL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new UpdateManagementCenterUrlOperation();
            }
        };
        constructors[UPDATE_MAP_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new UpdateMapConfigOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}

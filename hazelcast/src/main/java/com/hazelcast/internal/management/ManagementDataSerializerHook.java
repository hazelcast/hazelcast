/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.management.operation.ChangeClusterStateOperation;
import com.hazelcast.internal.management.operation.ReloadConfigOperation;
import com.hazelcast.internal.management.operation.SetLicenseOperation;
import com.hazelcast.internal.management.operation.UpdateConfigOperation;
import com.hazelcast.internal.management.operation.UpdateMapConfigOperation;
import com.hazelcast.internal.management.operation.UpdatePermissionConfigOperation;
import com.hazelcast.internal.management.operation.UpdateTcpIpMemberListOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper.Factory;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class ManagementDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = Factory.MANAGEMENT_DS.getFactoryId();

    public static final int UPDATE_MAP_CONFIG = 1;
    public static final int SET_LICENSE = 2;
    public static final int CHANGE_CLUSTER_STATE = 3;
    public static final int UPDATE_PERMISSION_CONFIG_OPERATION = 4;
    public static final int RELOAD_CONFIG_OPERATION = 5;
    public static final int UPDATE_CONFIG_OPERATION = 6;
    public static final int UPDATE_TCP_IP_MEMBER_LIST_OPERATION = 7;

    private static final int LEN = UPDATE_TCP_IP_MEMBER_LIST_OPERATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors
                = new ConstructorFunction[LEN];

        constructors[UPDATE_MAP_CONFIG] = arg -> new UpdateMapConfigOperation();
        constructors[SET_LICENSE] = arg -> new SetLicenseOperation();
        constructors[CHANGE_CLUSTER_STATE] = arg -> new ChangeClusterStateOperation();
        constructors[UPDATE_PERMISSION_CONFIG_OPERATION] = arg -> new UpdatePermissionConfigOperation();
        constructors[RELOAD_CONFIG_OPERATION] = arg -> new ReloadConfigOperation();
        constructors[UPDATE_CONFIG_OPERATION] = arg -> new UpdateConfigOperation();
        constructors[UPDATE_TCP_IP_MEMBER_LIST_OPERATION] = arg -> new UpdateTcpIpMemberListOperation();

        return new ArrayDataSerializableFactory(constructors);
    }
}

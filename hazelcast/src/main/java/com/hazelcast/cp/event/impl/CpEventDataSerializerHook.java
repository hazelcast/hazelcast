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

package com.hazelcast.cp.event.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public final class CpEventDataSerializerHook implements DataSerializerHook {

    private static final int CP_EVENT_DS_FACTORY_ID = -1020;
    private static final String CP_EVENT_DS_FACTORY = "hazelcast.serialization.ds.cp.event";

    public static final int F_ID = FactoryIdHelper.getFactoryId(CP_EVENT_DS_FACTORY, CP_EVENT_DS_FACTORY_ID);

    public static final int MEMBERSHIP_EVENT = 1;
    public static final int GROUP_AVAILABILITY_EVENT = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case MEMBERSHIP_EVENT:
                    return new CPMembershipEventImpl();
                case GROUP_AVAILABILITY_EVENT:
                    return new CPGroupAvailabilityEventImpl();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}

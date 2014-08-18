/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class ClusterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = Data.FACTORY_ID;

    public static final int DATA = Data.ID;
    public static final int ADDRESS = Address.ID;
    public static final int MEMBER = 2;
    public static final int HEARTBEAT = 3;
    public static final int CONFIG_CHECK = 4;
    public static final int CLIENT_CONFIG_CHECK = 5;

    // client
    public static final int MEMBERSHIP_EVENT = 8;

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
                    case DATA:
                        return new Data();
                    case ADDRESS:
                        return new Address();
                    case MEMBER:
                        return new MemberImpl();
                    case HEARTBEAT:
                        return new HeartbeatOperation();
                    case CONFIG_CHECK:
                        return new ConfigCheck();
                    case CLIENT_CONFIG_CHECK:
                        return new ClientConfigCheck();
                    case MEMBERSHIP_EVENT:
                        return new ClientMembershipEvent();
                    default:
                        return null;
                }
            }
        };
    }
}

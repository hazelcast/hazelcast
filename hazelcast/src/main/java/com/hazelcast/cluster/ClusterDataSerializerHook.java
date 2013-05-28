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

import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

/**
 * @mdogan 8/24/12
 */
public final class ClusterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = Data.FACTORY_ID;

    public static final int DATA = Data.ID;
    public static final int ADDRESS = Address.ID;
    public static final int MEMBER = 2;
    public static final int HEARTBEAT = 3;

    // client
    public static final int ADD_MS_LISTENER = 7;
    public static final int MEMBERSHIP_EVENT = 8;
    public static final int PING = 9;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable> ctors[] = new ConstructorFunction[10];
        ctors[DATA] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Data();
            }
        };

        ctors[ADDRESS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Address();
            }
        };

        ctors[MEMBER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberImpl();
            }
        };

        ctors[HEARTBEAT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HeartbeatOperation();
            }
        };


        ctors[ADD_MS_LISTENER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddMembershipListenerRequest();
            }
        };

        ctors[MEMBERSHIP_EVENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClientMembershipEvent();
            }
        };

        ctors[PING] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClientPingRequest();
            }
        };

        return new ArrayDataSerializableFactory(ctors);
    }
}

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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOperation;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatOperation;
import com.hazelcast.internal.cluster.impl.operations.MemberInfoUpdateOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class ClusterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = 0;

    public static final int ADDRESS = Address.ID;
    public static final int MEMBER = 2;
    public static final int HEARTBEAT = 3;
    public static final int CONFIG_CHECK = 4;
    public static final int BIND_MESSAGE = 5;

    public static final int MEMBER_INFO_UPDATE = 6;
    public static final int FINALIZE_JOIN = 7;

    private static final DataSerializableFactory FACTORY = new ClusterDataSerializerFactoryImpl();

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return FACTORY;
    }

    public static class ClusterDataSerializerFactoryImpl implements DataSerializableFactory {

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case ADDRESS:
                    return new Address();
                case MEMBER:
                    return new MemberImpl();
                case HEARTBEAT:
                    return new HeartbeatOperation();
                case CONFIG_CHECK:
                    return new ConfigCheck();
                case BIND_MESSAGE:
                    return new BindMessage();
                case MEMBER_INFO_UPDATE:
                    return new MemberInfoUpdateOperation();
                case FINALIZE_JOIN:
                    return new FinalizeJoinOperation();
                default:
                    return null;
            }
        }
    }
}

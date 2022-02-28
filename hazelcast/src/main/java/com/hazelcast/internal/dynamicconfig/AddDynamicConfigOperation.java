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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterTopologyChangedException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;

import java.io.IOException;

import static java.lang.String.format;

public class AddDynamicConfigOperation extends AbstractDynamicConfigOperation {

    private IdentifiedDataSerializable config;
    private int memberListVersion;

    public AddDynamicConfigOperation() {

    }

    public AddDynamicConfigOperation(IdentifiedDataSerializable config, int memberListVersion) {
        this.config = config;
        this.memberListVersion = memberListVersion;
    }

    @Override
    public void run() throws Exception {
        ClusterWideConfigurationService service = getService();
        service.registerConfigLocally(config, ConfigCheckMode.THROW_EXCEPTION);
        ClusterService clusterService = getNodeEngine().getClusterService();
        if (clusterService.isMaster()) {
            int currentMemberListVersion = clusterService.getMemberListVersion();
            if (currentMemberListVersion != memberListVersion) {
                throw new ClusterTopologyChangedException(
                        format("Current member list version %d does not match expected %d", currentMemberListVersion,
                                memberListVersion));
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(config);
        out.writeInt(memberListVersion);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        config = in.readObject();
        memberListVersion = in.readInt();
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.ADD_DYNAMIC_CONFIG_OP;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return (throwable instanceof ClusterTopologyChangedException) ? ExceptionAction.THROW_EXCEPTION
                : super.onInvocationException(throwable);
    }
}

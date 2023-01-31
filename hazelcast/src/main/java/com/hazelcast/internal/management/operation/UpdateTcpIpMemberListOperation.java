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

package com.hazelcast.internal.management.operation;

import com.hazelcast.config.Config;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService;
import com.hazelcast.internal.dynamicconfig.ConfigurationService;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Operation to update the member list in the tcp-ip join config at runtime.
 */
public class UpdateTcpIpMemberListOperation extends AbstractManagementOperation  {
    private List<String> members;

    public UpdateTcpIpMemberListOperation() {
    }

    public UpdateTcpIpMemberListOperation(List<String> members) {
        this.members = members;
    }

    @Override
    public void run() throws Exception {
        Config config = ((NodeEngineImpl) getNodeEngine()).getNode().getConfig();
        Object activeNetworkConfig;
        TcpIpConfig tcpIpConfig;
        if (config.getAdvancedNetworkConfig().isEnabled()) {
            activeNetworkConfig = config.getAdvancedNetworkConfig();
            tcpIpConfig = config.getAdvancedNetworkConfig().getJoin().getTcpIpConfig();
        } else {
            activeNetworkConfig = config.getNetworkConfig();
            tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
        }
        if (tcpIpConfig.isEnabled()) {
            tcpIpConfig.setMembers(members);
            try {
                ConfigurationService configurationService
                        = getNodeEngine().getService(ClusterWideConfigurationService.SERVICE_NAME);
                configurationService.persist(activeNetworkConfig);
            } catch (Exception ignored) {
            }
        } else {
            throw new IllegalStateException("TCP-IP join mechanism is not enabled in the cluster.");
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        members = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String member = in.readString();
            members.add(member);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(members.size());
        for (String member : members) {
            out.writeString(member);
        }
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.UPDATE_TCP_IP_MEMBER_LIST_OPERATION;
    }

}

/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class AuthorizationOp extends AbstractJoinOperation {

    private String groupName;
    private String groupPassword;
    private Boolean response = Boolean.TRUE;

    public AuthorizationOp() {
    }

    public AuthorizationOp(String groupName, String groupPassword) {
        this.groupName = groupName;
        this.groupPassword = groupPassword;
    }

    @Override
    public void run() {
        GroupConfig groupConfig = getNodeEngine().getConfig().getGroupConfig();
        if (!groupName.equals(groupConfig.getName())) {
            response = Boolean.FALSE;
        } else if (!groupPassword.equals(groupConfig.getPassword())) {
            response = Boolean.FALSE;
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
        groupPassword = in.readUTF();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.AUTHORIZATION;
    }
}

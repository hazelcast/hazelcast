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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public final class ClientConfigCheck implements IdentifiedDataSerializable {

    private String groupName;

    private String groupPassword;

    public ClientConfigCheck() {
    }

    public boolean isCompatible(ClientConfigCheck other) {
        if (!groupName.equals(other.groupName)) {
            return false;
        }
        if (!groupPassword.equals(other.groupPassword)) {
            throw new HazelcastException("Incompatible group password!");
        }

        return true;
    }

    public ClientConfigCheck setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public ClientConfigCheck setGroupPassword(String groupPassword) {
        this.groupPassword = groupPassword;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.CLIENT_CONFIG_CHECK;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
        groupPassword = in.readUTF();
    }
}

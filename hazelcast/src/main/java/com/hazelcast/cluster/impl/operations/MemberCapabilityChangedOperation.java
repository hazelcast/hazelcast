/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl.operations;

import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.instance.Capability;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Set;

public class MemberCapabilityChangedOperation extends AbstractClusterOperation {


    private String uuid;
    private Set<Capability> capabilities;

    public MemberCapabilityChangedOperation() {
    }

    public MemberCapabilityChangedOperation(String uuid, Set<Capability> capabilities) {
        this.uuid = uuid;
        this.capabilities = capabilities;
    }

    public String getUuid() {
        return uuid;
    }

    public Set<Capability> getCapabilities() {
        return capabilities;
    }

    @Override
    public void run() throws Exception {
        ((ClusterServiceImpl) getService()).updateMemberCapabilities(uuid, capabilities);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(uuid);
        Capability.writeCapabilities(out, capabilities);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        uuid = in.readUTF();

        capabilities = Capability.readCapabilities(in);
    }
}

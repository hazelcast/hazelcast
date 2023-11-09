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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;

public class WipeDestroyedObjectsOp extends RaftOp implements IdentifiedDataSerializable {
    @Override
    public Void run(CPGroupId groupId, long commitIndex) throws Exception {
        clearDestroyedNamesInAllServices(groupId);
        return null;
    }

    private void clearDestroyedNamesInAllServices(CPGroupId groupId) {
        Collection<RaftRemoteService> services = getNodeEngine().getServices(RaftRemoteService.class);
        services.forEach(service -> service.clearDestroyedNames(groupId));
    }

    @Override
    protected String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.WIPE_DESTROYED_OBJECTS_OP;
    }
}

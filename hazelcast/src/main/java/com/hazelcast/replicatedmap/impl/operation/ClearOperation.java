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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.INVOCATION_TRY_COUNT;

/**
 * This operation will execute the remote clear on replicated map if
 * {@link ReplicatedMap#clear()} is called.
 */
public class ClearOperation extends AbstractNamedSerializableOperation implements MutatingOperation {

    private String mapName;
    private boolean replicateClear;
    private long version;
    private transient int response;

    public ClearOperation() {
    }

    public ClearOperation(String mapName, boolean replicateClear) {
        this(mapName, replicateClear, 0);
    }

    public ClearOperation(String mapName, boolean replicateClear, long version) {
        this.mapName = mapName;
        this.replicateClear = replicateClear;
        this.version = version;
    }

    @Override
    public void run() throws Exception {
        if (getNodeEngine().getLocalMember().isLiteMember()) {
            return;
        }
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(mapName, false, getPartitionId());
        if (store == null) {
            return;
        }
        response = store.getStorage().size();

        if (replicateClear) {
            store.clear();
            replicateClearOperation(version);
        } else {
            store.clearWithVersion(version);
        }
    }

    private void replicateClearOperation(long version) {
        OperationService operationService = getNodeEngine().getOperationService();
        Collection<Address> members = getMemberAddresses();
        for (Address address : members) {
            Operation op = new ClearOperation(mapName, false, version)
                    .setPartitionId(getPartitionId())
                    .setValidateTarget(false);
            operationService
                    .createInvocationBuilder(getServiceName(), op, address)
                    .setTryCount(INVOCATION_TRY_COUNT)
                    .invoke();
        }
    }

    protected Collection<Address> getMemberAddresses() {
        Address thisAddress = getNodeEngine().getThisAddress();
        Collection<Member> members = getNodeEngine().getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        Collection<Address> addresses = new ArrayList<>();
        for (Member member : members) {
            Address address = member.getAddress();
            if (address.equals(thisAddress)) {
                continue;
            }
            addresses.add(address);
        }
        return addresses;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.CLEAR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(mapName);
        out.writeBoolean(replicateClear);
        out.writeLong(version);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readString();
        replicateClear = in.readBoolean();
        version = in.readLong();
    }

    @Override
    public String getName() {
        return mapName;
    }
}

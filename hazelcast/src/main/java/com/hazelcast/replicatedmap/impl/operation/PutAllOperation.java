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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEventPublishingService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntries;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Puts a set of records to the replicated map.
 */
public class PutAllOperation extends AbstractSerializableOperation {

    private String name;
    private ReplicatedMapEntries entries;

    @SuppressWarnings("unused")
    public PutAllOperation() {
    }

    public PutAllOperation(String name, ReplicatedMapEntries entries) {
        this.name = name;
        this.entries = entries;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        int partitionId = getPartitionId();
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        for (int i = 0; i < entries.size(); i++) {
            Data key = entries.getKey(i);
            Data value = entries.getValue(i);
            if (partitionId != partitionService.getPartitionId(key)) {
                continue;
            }
            Object putResult = store.put(key, value);
            Data oldValue = getNodeEngine().toData(putResult);
            eventPublishingService.fireEntryListenerEvent(key, oldValue, value, name, getCallerAddress());
            VersionResponsePair response = new VersionResponsePair(putResult, store.getVersion());
            publishReplicationMessage(key, value, response);
        }
    }

    private void publishReplicationMessage(Data key, Data value, VersionResponsePair response) {
        OperationService operationService = getNodeEngine().getOperationService();
        Collection<Member> members = getNodeEngine().getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        for (Member member : members) {
            Address address = member.getAddress();
            if (address.equals(getNodeEngine().getThisAddress())) {
                continue;
            }
            ReplicateUpdateOperation updateOperation = new ReplicateUpdateOperation(name, key, value, 0, response, false,
                    getCallerAddress());
            updateOperation.setPartitionId(getPartitionId());
            updateOperation.setValidateTarget(false);
            operationService.invokeOnTarget(getServiceName(), updateOperation, address);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(entries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        entries = in.readObject();
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.PUT_ALL;
    }
}

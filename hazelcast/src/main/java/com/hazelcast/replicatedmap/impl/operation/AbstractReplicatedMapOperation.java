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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import java.util.ArrayList;
import java.util.Collection;


public abstract class AbstractReplicatedMapOperation extends AbstractOperation {

    private static ILogger logger = Logger.getLogger(AbstractReplicatedMapOperation.class.getName());

    protected String name;
    protected Data key;
    protected Data value;
    protected long ttl;
    protected transient VersionResponsePair response;


    protected void sendReplicationOperation(final boolean isRemove) {
        final OperationService operationService = getNodeEngine().getOperationService();
        Collection<Address> members = getMemberAddresses();
        for (Address address : members) {
            invoke(isRemove, operationService, address, name, key, value, ttl, response);
        }
    }

    protected Collection<Address> getMemberAddresses() {
        Address thisAddress = getNodeEngine().getThisAddress();
        Collection<Member> members = getNodeEngine().getClusterService().getMembers();
        Collection<Address> addresses = new ArrayList<Address>();
        for (Member member : members) {
            Address address = member.getAddress();
            if (address.equals(getCallerAddress()) || address.equals(thisAddress)) {
                continue;
            }
            addresses.add(address);
        }
        return addresses;
    }

    private void invoke(boolean isRemove, OperationService operationService, Address address, String name, Data key,
                        Data value, long ttl, VersionResponsePair response) {
        ReplicateUpdateOperation updateOperation = new ReplicateUpdateOperation(name, key, value, ttl, response,
                isRemove);
        updateOperation.setPartitionId(getPartitionId());
        updateOperation.setValidateTarget(false);
        operationService.invokeOnTarget(getServiceName(), updateOperation, address);
    }

    protected void sendUpdateCallerOperation(boolean isRemove) {
        OperationService operationService = getNodeEngine().getOperationService();
        ReplicateUpdateToCallerOperation updateCallerOperation = new ReplicateUpdateToCallerOperation(name, getCallId(),
                key, value, response, ttl, isRemove);
        updateCallerOperation.setPartitionId(getPartitionId());
        updateCallerOperation.setValidateTarget(false);
        updateCallerOperation.setServiceName(getServiceName());
        operationService.invokeOnTarget(getServiceName(), updateCallerOperation, getCallerAddress());
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        if (getNodeEngine().getThisAddress().equals(getCallerAddress())) {
            return response;
        } else {
            NormalResponse resp = new NormalResponse(response, getCallId(), 1, isUrgent());
            return resp;
        }
    }
}

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

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.INVOCATION_TRY_COUNT;


public abstract class AbstractReplicatedMapOperation extends Operation {

    protected String name;
    protected Data key;
    protected Data value;
    protected long ttl;
    protected transient VersionResponsePair response;

    private transient boolean postponeReply;

    private enum CallbackResponseMode {
        /**
         * Callback does not have to send a response -> the operation itself will return the response
         *
         */
        DO_NOT_SEND_RESPONSE,

        /**
         * Callback will send the response, but the response will not set expectedAck. This is useful
         * in cases where we are updating a local portion of the replicated map
         *
         */
        SEND_PLAIN_RESPONSE
    }

    protected void sendReplicationOperation(final boolean isRemove) {
        final OperationService operationService = getNodeEngine().getOperationService();
        Collection<Address> members = getMemberAddresses();
        if (members.size() == 0) {
            return;
        }

        ReplicatedMapService service = getService();
        CallbackResponseMode callbackResponseMode = resolveCallbackResponseMode(service);

        ExecutionCallback callback = new ReplicatedMapCallback(this, members.size(), callbackResponseMode);
        for (Address address : members) {
            ReplicateUpdateOperation operation = new ReplicateUpdateOperation(name, key, value, ttl, response,
                    isRemove, getCallerAddress());
            invoke(operation, operationService, address, callback);
            service.replicateUpdateOperationStarted();
        }
    }

    private OperationServiceImpl getOperationServiceImpl() {
        return (OperationServiceImpl) getNodeEngine().getOperationService();
    }

    private CallbackResponseMode resolveCallbackResponseMode(ReplicatedMapService service) {
        boolean backpressureNeeded = service.isBackpressureNeeded();
        if (backpressureNeeded) {
            postponeReply = true;
            getOperationServiceImpl().onStartAsyncOperation(this);
            return CallbackResponseMode.SEND_PLAIN_RESPONSE;
        }
        return CallbackResponseMode.DO_NOT_SEND_RESPONSE;
    }

    protected Collection<Address> getMemberAddresses() {
        Address thisAddress = getNodeEngine().getThisAddress();
        Collection<Member> members = getNodeEngine().getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
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

    private void invoke(ReplicateUpdateOperation updateOperation, OperationService operationService,
                        Address address, ExecutionCallback callback) {
        updateOperation.setPartitionId(getPartitionId());
        updateOperation.setValidateTarget(false);
        InvocationBuilder invocation = operationService
                .createInvocationBuilder(getServiceName(), updateOperation, address)
                .setTryCount(INVOCATION_TRY_COUNT)
                .setExecutionCallback(callback);
        invocation.invoke();

    }

    protected void sendUpdateCallerOperation(boolean isRemove) {
        OperationService operationService = getNodeEngine().getOperationService();
        ReplicateUpdateToCallerOperation updateCallerOperation = new ReplicateUpdateToCallerOperation(name, getCallId(),
                key, value, response, ttl, isRemove);
        updateCallerOperation.setPartitionId(getPartitionId());
        updateCallerOperation.setValidateTarget(false);
        updateCallerOperation.setServiceName(getServiceName());
        operationService
                .createInvocationBuilder(getServiceName(), updateCallerOperation, getCallerAddress())
                .setTryCount(INVOCATION_TRY_COUNT)
                .invoke();
    }

    @Override
    public boolean returnsResponse() {
        return !postponeReply;
    }

    @Override
    public Object getResponse() {
        if (getNodeEngine().getThisAddress().equals(getCallerAddress())) {
            return response;
        } else {
            return new NormalResponse(response, getCallId(), 1, isUrgent());
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }

    private final class ReplicatedMapCallback extends SimpleExecutionCallback<Object> {
        private final AbstractReplicatedMapOperation op;
        private final AtomicInteger count;
        private final CallbackResponseMode callbackResponseMode;
        private final ReplicatedMapService service;

        private ReplicatedMapCallback(AbstractReplicatedMapOperation op, int count, CallbackResponseMode callbackResponseMode) {
            this.op = op;
            this.count = new AtomicInteger(count);
            this.callbackResponseMode = callbackResponseMode;
            this.service = getService();
        }

        @Override
        public void notify(Object ignored) {
            service.replicateUpdateOperationCompleted();
            if (count.decrementAndGet() == 0) {
                lastResponseReceived();
            }
        }

        private void lastResponseReceived() {
            switch (callbackResponseMode) {
                case SEND_PLAIN_RESPONSE:
                    op.sendResponse(getResponse());
                    getOperationServiceImpl().onCompletionAsyncOperation(AbstractReplicatedMapOperation.this);
                    break;
                case DO_NOT_SEND_RESPONSE:
                    break;
                default:
                    getLogger().warning("Unexpected response mode " + callbackResponseMode);
            }
        }
    }
}

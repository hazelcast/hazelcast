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

package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.JobPermission;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;

public class JetGetJobIdsMessageTask extends AbstractMultiTargetMessageTask<JetGetJobIdsCodec.RequestParameters> {

    private transient Address masterAddress;

    JetGetJobIdsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return () -> new GetJobIdsOperation(parameters.onlyName, parameters.onlyJobId);
    }

    @Override
    public Collection<Member> getTargets() {
        masterAddress = nodeEngine.getClusterService().getMasterAddress();
        if (masterAddress == null) {
            throw new IllegalStateException("Master is not known yet");
        }

        // if onlyName != null, only send the operation to master. Light jobs cannot have a name
        if (parameters.onlyName != null) {
            Member masterMember = nodeEngine.getClusterService().getMember(masterAddress);
            if (masterMember == null) {
                throw new IllegalStateException("Master changed");
            }
            return singleton(masterMember);
        } else {
            return nodeEngine.getClusterService().getMembers();
        }
    }

    @Override
    protected Object reduce(Map<Member, Object> map) {
        return map.entrySet().stream().collect(toMap(
                en -> en.getKey().getUuid(),
                en -> {
                    Object response = en.getValue();
                    if (response instanceof MemberLeftException
                            || response instanceof TargetNotMemberException
                            || response instanceof TargetDisconnectedException) {
                        return GetJobIdsResult.EMPTY;
                    }
                    if (response instanceof Exception) {
                        // ignore exceptions from non-master, but not from master
                        if (en.getKey().getAddress().equals(masterAddress)) {
                            throw new RuntimeException((Throwable) response);
                        }
                        return GetJobIdsResult.EMPTY;
                    }
                    return response;
                }));
    }

    @Override
    protected JetGetJobIdsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetGetJobIdsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetGetJobIdsCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getJobIds";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public String getServiceName() {
        return JetServiceBackend.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new JobPermission(ActionConstants.ACTION_READ);
    }
}

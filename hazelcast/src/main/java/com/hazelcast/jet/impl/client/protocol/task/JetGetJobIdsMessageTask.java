/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.task.AbstractAsyncMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.JobPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.util.stream.Collectors.toMap;

public class JetGetJobIdsMessageTask extends AbstractAsyncMessageTask<JetGetJobIdsCodec.RequestParameters, Object> {

    JetGetJobIdsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Object> processInternal() {

        GetJobIdsOperation masterOperation = new GetJobIdsOperation(parameters.onlyName, parameters.onlyJobId, false);
        var masterInvocation = nodeEngine
                .getOperationService()
                .createMasterInvocationBuilder(JetServiceBackend.SERVICE_NAME, masterOperation)
                .build();
        CompletableFuture<GetJobIdsResult> masterFuture = masterInvocation.invoke();

        // perform operation only on master node
        if (parameters.onlyName != null) {
            return masterFuture.handleAsync(
                    (result, throwable) -> {
                        if (throwable != null) {
                            sneakyThrow(throwable);
                        }
                        Map<UUID, GetJobIdsResult> results = createHashMap(1);
                        results.put(masterInvocation.getTargetMember().getUuid(), result);
                        return results;
                    },
                    CALLER_RUNS
            );
        }

        // get info only about light jobs from all nodes
        Supplier<Operation> operationSupplier = () -> new GetJobIdsOperation(
                parameters.onlyName,
                parameters.onlyJobId,
                true
        ).setCallerUuid(endpoint.getUuid());

        var allMembersFuture = InvocationUtil.invokeAndReduceOnAllClusterMembers(
                nodeEngine,
                operationSupplier,
                (r, t) -> {
                    if (t == null) {
                        return r;
                    }
                    // this is inconsistent with member api
                    // ignore all exceptions about light jobs
                    return GetJobIdsResult.EMPTY;
                },
                m -> m.entrySet().stream()
                        .collect(
                                toMap(
                                        en -> en.getKey().getUuid(),
                                        Map.Entry::getValue
                                )
                        )
        );

        return masterFuture.thenCombine(
                allMembersFuture,
                (masterResult, allMemberResults) -> {
                    allMemberResults.put(masterInvocation.getTargetMember().getUuid(), masterResult);
                    return allMemberResults;
                }
        );
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

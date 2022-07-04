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

package com.hazelcast.cp.internal.datastructures.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp.AlterResultType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;

import java.security.Permission;

/**
 * Client message task for {@link AlterOp}
 */
public class AlterMessageTask extends AbstractCPMessageTask<AtomicLongAlterCodec.RequestParameters> {

    public AlterMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        IFunction<Long, Long> function = serializationService.toObject(parameters.function);
        AlterResultType resultType = AlterResultType.fromValue(parameters.returnValueType);
        invoke(parameters.groupId, new AlterOp(parameters.name, function, resultType));
    }

    @Override
    protected AtomicLongAlterCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return AtomicLongAlterCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return AtomicLongAlterCodec.encodeResponse((Long) response);
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicLongPermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        if (parameters.returnValueType == AlterResultType.OLD_VALUE.value()) {
            return "getAndAlter";
        }
        if (parameters.returnValueType == AlterResultType.NEW_VALUE.value()) {
            return "alterAndGet";
        }
        return "alter";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.function};
    }
}

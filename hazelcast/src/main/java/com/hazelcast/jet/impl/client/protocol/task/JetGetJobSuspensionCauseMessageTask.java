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
import com.hazelcast.client.impl.protocol.codec.JetGetJobSuspensionCauseCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.operation.GetJobSuspensionCauseOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;

public class JetGetJobSuspensionCauseMessageTask extends AbstractJetMessageTask<Long, Data> {

    JetGetJobSuspensionCauseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                JetGetJobSuspensionCauseCodec::decodeRequest,
                JetGetJobSuspensionCauseCodec::encodeResponse);
    }

    @Override
    protected ClientMessage encodeResponse(Object o) {
        return super.encodeResponse(toData(o));
    }

    @Override
    protected Operation prepareOperation() {
        return new GetJobSuspensionCauseOperation(parameters);
    }

    @Override
    public String getMethodName() {
        return "getJobSuspensionCause";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Nullable
    @Override
    public String[] actions() {
        return new String[]{ActionConstants.ACTION_READ};
    }
}

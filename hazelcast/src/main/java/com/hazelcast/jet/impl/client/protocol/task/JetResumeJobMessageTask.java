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
import com.hazelcast.client.impl.protocol.codec.JetResumeJobCodec;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.impl.operation.ResumeJobOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;

public class JetResumeJobMessageTask
        extends AbstractJetMessageTask<Long, Void> implements BlockingMessageTask {
    protected JetResumeJobMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection, JetResumeJobCodec::decodeRequest,
                o -> JetResumeJobCodec.encodeResponse());
    }

    @Override
    protected Operation prepareOperation() {
        return new ResumeJobOperation(parameters);
    }

    @Override
    public String getMethodName() {
        return "resumeJob";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{};
    }

    @Nullable
    @Override
    public String[] actions() {
        return new String[]{ActionConstants.ACTION_RESTART};
    }
}

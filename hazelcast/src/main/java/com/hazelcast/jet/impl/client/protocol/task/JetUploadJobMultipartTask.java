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
import com.hazelcast.client.impl.protocol.codec.JetUploadJobMultipartCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.impl.operation.UploadJobMultiPartOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;

/**
 * The task that creates UploadJobMultiPartOperation with client protocol message
 */
public class JetUploadJobMultipartTask extends
        AbstractJetMessageTask<JetUploadJobMultipartCodec.RequestParameters, Boolean> {

    protected JetUploadJobMultipartTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                JetUploadJobMultipartCodec::decodeRequest,
                JetUploadJobMultipartCodec::encodeResponse);
    }

    @Override
    @Nullable
    public String[] actions() {
        return new String[]{ActionConstants.ACTION_ADD_RESOURCES};
    }

    @Override
    protected Operation prepareOperation() {
        return new UploadJobMultiPartOperation(parameters.sessionId,
                parameters.currentPartNumber,
                parameters.totalPartNumber,
                parameters.partData,
                parameters.partSize);
    }

    @Override
    public String getMethodName() {
        return "uploadJobData";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}

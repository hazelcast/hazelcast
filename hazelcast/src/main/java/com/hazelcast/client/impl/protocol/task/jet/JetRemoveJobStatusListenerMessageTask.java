/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.jet;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetRemoveJobStatusListenerCodec;
import com.hazelcast.client.impl.protocol.codec.JetRemoveJobStatusListenerCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractRemoveListenerMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.impl.JobService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.JobPermission;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.JobProxy.checkJobStatusListenerSupported;

public class JetRemoveJobStatusListenerMessageTask extends AbstractRemoveListenerMessageTask<RequestParameters> {

    public JetRemoveJobStatusListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Future<Boolean> deRegisterListener() {
        checkJobStatusListenerSupported(nodeEngine);
        JobService jobService = getService(JobService.SERVICE_NAME);
        return jobService.removeEventListenerAsync(parameters.jobId, parameters.registrationId);
    }

    @Override
    protected UUID getRegistrationId() {
        return parameters.registrationId;
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetRemoveJobStatusListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetRemoveJobStatusListenerCodec.encodeResponse((boolean) response);
    }

    @Override
    public String getServiceName() {
        return JobService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "removeJobStatusListener";
    }

    @Override
    public String getDistributedObjectName() {
        return idToString(parameters.jobId);
    }

    @Override
    public Permission getRequiredPermission() {
        return new JobPermission(ActionConstants.ACTION_LISTEN);
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

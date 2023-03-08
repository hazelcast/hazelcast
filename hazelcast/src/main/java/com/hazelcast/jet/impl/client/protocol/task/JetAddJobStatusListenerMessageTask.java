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

package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetAddJobStatusListenerCodec;
import com.hazelcast.client.impl.protocol.codec.JetAddJobStatusListenerCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractAddListenerMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.JobStatusEvent;
import com.hazelcast.jet.JobStatusListener;
import com.hazelcast.jet.impl.JobEventService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.JobPermission;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.JobProxy.checkJobStatusListenerSupported;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class JetAddJobStatusListenerMessageTask extends AbstractAddListenerMessageTask<RequestParameters> {

    public JetAddJobStatusListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        checkJobStatusListenerSupported(nodeEngine);
        JobEventService jobEventService = getService(JobEventService.SERVICE_NAME);
        long jobId = parameters.jobId;
        JobStatusListener listener = new ClientJobStatusListener();
        return parameters.localOnly
                ? newCompletedFuture(jobEventService.addLocalEventListener(jobId, listener))
                : jobEventService.addEventListenerAsync(jobId, listener);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetAddJobStatusListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetAddJobStatusListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return JobEventService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "addJobStatusListener";
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

    private class ClientJobStatusListener implements JobStatusListener {
        @Override
        public void jobStatusChanged(JobStatusEvent e) {
            if (endpoint.isAlive()) {
                sendClientMessage(e.getJobId(), JetAddJobStatusListenerCodec.encodeJobStatusEvent(
                        e.getJobId(), e.getPreviousStatus().getId(), e.getNewStatus().getId(),
                        e.getDescription(), e.isUserRequested()));
            }
        }
    }
}

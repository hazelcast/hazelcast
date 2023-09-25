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
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.JobStatusEvent;
import com.hazelcast.jet.JobStatusListener;
import com.hazelcast.jet.impl.JobEventService;
import com.hazelcast.jet.impl.operation.AddJobStatusListenerOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.impl.eventservice.impl.Registration;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.UUID;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.JobProxy.checkJobStatusListenerSupported;

public class JetAddJobStatusListenerMessageTask extends AbstractJetMessageTask<RequestParameters, UUID> {

    public JetAddJobStatusListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                JetAddJobStatusListenerCodec::decodeRequest,
                JetAddJobStatusListenerCodec::encodeResponse);
    }

    @Override
    protected UUID getLightJobCoordinator() {
        return parameters.lightJobCoordinator;
    }

    @Override
    protected Operation prepareOperation() {
        checkJobStatusListenerSupported(nodeEngine);
        long jobId = parameters.jobId;
        JobEventService jobEventService = nodeEngine.getService(JobEventService.SERVICE_NAME);
        Registration registration = jobEventService.prepareRegistration(
                jobId, new ClientJobStatusListener(), parameters.localOnly);
        return new AddJobStatusListenerOperation(
                jobId, parameters.lightJobCoordinator != null, registration);
    }

    @Override
    protected Object processResponseBeforeSending(Object response) {
        endpoint.addListenerDestroyAction(getServiceName(), getDistributedObjectName(), (UUID) response);
        return response;
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
    public String[] actions() {
        return new String[] {ActionConstants.ACTION_LISTEN};
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

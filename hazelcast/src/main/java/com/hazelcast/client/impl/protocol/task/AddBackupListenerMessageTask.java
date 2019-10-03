/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.ClientBackupService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientLocalBackupListenerCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import java.security.Permission;
import java.util.UUID;

public class AddBackupListenerMessageTask
        extends AbstractCallableMessageTask<ClientLocalBackupListenerCodec.RequestParameters> {


    public AddBackupListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    public class BackupListener {
        public void onEvent(Long backupId) {
            ClientMessage eventMessage = ClientLocalBackupListenerCodec.encodeBackupEvent(backupId);
            eventMessage.getStartFrame().flags |= ClientMessage.BACKUP_EVENT_FLAG;
            sendClientMessage(eventMessage);
        }
    }

    @Override
    protected Object call() {
        EventService eventService = clientEngine.getEventService();
        UUID uuid = endpoint.getUuid();
        logger.info("Client as adding backup listener. client uuid " + uuid);
        String serviceName = ClientBackupService.SERVICE_NAME;
        EventRegistration registration =
                eventService.registerLocalListener(serviceName, serviceName, new BackupListener());
        endpoint.addListenerDestroyAction(serviceName, serviceName, registration.getId());
        return registration.getId();
    }

    @Override
    protected ClientLocalBackupListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientLocalBackupListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientLocalBackupListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    public Permission getRequiredPermission() {
        return null;
    }

}

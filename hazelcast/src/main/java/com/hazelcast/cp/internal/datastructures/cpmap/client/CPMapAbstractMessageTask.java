/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapService;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapOperationProvider;
import com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

public abstract class CPMapAbstractMessageTask<P> extends AbstractCPMessageTask<P> {
    protected CPMapAbstractMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public String getServiceName() {
        return CPMapService.SERVICE_NAME;
    }

    protected CPMapOperationProvider getCpMapOperationProvider(CPGroupId groupId, String objectName) {
        CPMapService service = getService(CPMapService.SERVICE_NAME);
        CPMapStore mapStore = service.getOrInitMapStore(groupId, objectName);
        boolean purgeEnabled = mapStore.isPurgeEnabled();
        return service.getCpMapOperationProvider(purgeEnabled);
    }
}

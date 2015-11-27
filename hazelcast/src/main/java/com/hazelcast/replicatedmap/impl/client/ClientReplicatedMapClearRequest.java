/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEventPublishingService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.ClearOperationFactory;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.security.Permission;
import java.util.Map;

/**
 * Client request class for {@link java.util.Map#clear()} implementation
 */
public class ClientReplicatedMapClearRequest extends AllPartitionsClientRequest {

    private String mapName;

    ClientReplicatedMapClearRequest() {
    }

    public ClientReplicatedMapClearRequest(String name) {
        this.mapName = name;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new ClearOperationFactory(mapName);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int deletedEntrySize = 0;
        for (Object deletedEntryPerPartition : map.values()) {
            deletedEntrySize += (Integer) deletedEntryPerPartition;
        }
        ReplicatedMapService service = getService();
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        eventPublishingService.fireMapClearedEvent(deletedEntrySize, getDistributedObjectName());
        return null;
    }


    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.CLEAR;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(mapName, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return mapName;
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "clear";
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", mapName);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        mapName = reader.readUTF("n");
    }
}

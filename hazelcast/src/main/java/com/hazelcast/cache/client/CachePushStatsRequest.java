/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.client;

import com.hazelcast.cache.CachePortableHook;
import com.hazelcast.cache.CacheService;
import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.instance.Node;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.monitor.TimedClientState;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.security.Permission;

/**
 * date: 13/03/14
 * author: eminn
 */
public class CachePushStatsRequest extends CallableClientRequest {

    private TimedClientState clientState;

    public CachePushStatsRequest() {
    }

    public CachePushStatsRequest(TimedClientState clientState) {
        this.clientState = clientState;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.SEND_STATS;
    }

    @Override
    public Object call() throws Exception {
        final CacheService service = getService();
        final NodeEngine nodeEngine = service.getNodeEngine();
        final Node node = ((NodeEngineImpl) nodeEngine).getNode();
        final ManagementCenterService managementCenterService = node.getManagementCenterService();
        if (managementCenterService != null) {
//            managementCenterService.addClientState(clientState);
        }
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        final ObjectDataOutput out = writer.getRawDataOutput();
        clientState.writeData(out);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        final ObjectDataInput in = reader.getRawDataInput();
        clientState = new TimedClientState();
        clientState.readData(in);
    }


    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}

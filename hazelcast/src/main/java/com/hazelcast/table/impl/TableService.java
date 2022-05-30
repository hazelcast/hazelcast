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

package com.hazelcast.table.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.table.TableProxy;

import java.util.Properties;
import java.util.UUID;

public class TableService implements PartitionAwareOperation, ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:tableService";
    private final NodeEngineImpl nodeEngine;

    public TableService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {

    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
        return new TableProxy(nodeEngine, this, objectName);
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
        throw new RuntimeException();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source) {
        throw new RuntimeException();
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        throw new RuntimeException();
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public int getPartitionId() {
        return 0;
    }
}

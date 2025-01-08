/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngine;

import static com.hazelcast.internal.namespace.NamespaceUtil.callWithNamespace;

/**
 * This is a {@link Data} based {@link ReplicatedRecordStore}
 * implementation
 */
public class DataReplicatedRecordStore extends AbstractReplicatedRecordStore<Data, Data> {

    private final NodeEngine nodeEngine;

    public DataReplicatedRecordStore(String name, ReplicatedMapService replicatedMapService, int partitionId) {
        super(name, replicatedMapService, partitionId);
        this.nodeEngine = replicatedMapService.getNodeEngine();
    }

    @Override
    public Object unmarshall(Object object) {
        String userCodeNamespace = replicatedMapConfig.getUserCodeNamespace();
        return object == null ? null
                : callWithNamespace(nodeEngine, userCodeNamespace,  () -> nodeEngine.toObject(object));
    }

    @Override
    public Object marshall(Object object) {
        String userCodeNamespace = replicatedMapConfig.getUserCodeNamespace();
        return callWithNamespace(nodeEngine, userCodeNamespace,  () -> nodeEngine.toData(object));
    }
}

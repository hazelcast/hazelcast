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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.ClusterMetadata;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReadonlyOperation;

public class GetClusterMetadataOperation extends Operation implements
        IdentifiedDataSerializable,
        ReadonlyOperation {

    private ClusterMetadata response;

    public GetClusterMetadataOperation() {
    }

    @Override
    public void run() {
        JetService service = getService();
        HazelcastInstance instance = service.getJetInstance().getHazelcastInstance();
        Cluster cluster = instance.getCluster();
        String name = instance.getConfig().getGroupConfig().getName();
        response = new ClusterMetadata(name, cluster);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.GET_CLUSTER_METADATA_OP;
    }

}

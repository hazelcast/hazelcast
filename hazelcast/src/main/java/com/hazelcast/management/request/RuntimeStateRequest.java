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

package com.hazelcast.management.request;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.concurrent.lock.LockResource;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.management.ClusterRuntimeState;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.partition.InternalPartitionService;
import java.util.Collection;
import java.util.Map;

public class RuntimeStateRequest implements ConsoleRequest {

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_STATE;
    }

    private Collection<LockResource> collectLockState(HazelcastInstanceImpl instance) {
        LockService lockService = instance.node.nodeEngine.getService(LockService.SERVICE_NAME);
        return lockService.getAllLocks();
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject os) {
//        HazelcastInstanceImpl instance = mcs.getHazelcastInstance();
//        Node node = instance.node;
//        ClusterServiceImpl cluster = node.getClusterService();
//        InternalPartitionService partitionService = node.partitionService;
//        Collection<LockResource> lockedRecords = collectLockState(instance);
//        Map<Address, Connection> connectionMap = node.connectionManager.getReadonlyConnectionMap();
//
//        ClusterRuntimeState clusterRuntimeState = new ClusterRuntimeState(
//                cluster.getMembers(),
//                partitionService.getPartitions(),
//                partitionService.getActiveMigrations(),
//                connectionMap,
//                lockedRecords);
//        clusterRuntimeState.writeData(dos);
    }

    @Override
    public Object readResponse(JsonObject in) {
//        ClusterRuntimeState clusterRuntimeState = new ClusterRuntimeState();
//        clusterRuntimeState.readData(in);
//        return clusterRuntimeState;
        return null;
    }

    @Override
    public JsonValue toJson() {
        return new JsonObject();
    }

    @Override
    public void fromJson(JsonObject json) {

    }
}

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

package com.hazelcast.semaphore;

import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.ResponseHandlerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 1/21/13
 */
public class SemaphoreService implements ManagedService, MigrationAwareService, MembershipAwareService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:semaphoreService";

    private final ConcurrentMap<String, Permit> permitMap = new ConcurrentHashMap<String, Permit>();

    private final NodeEngine nodeEngine;

    public SemaphoreService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public Permit getOrCreatePermit(String name){
        Permit permit = permitMap.get(name);
        if (permit == null){
            SemaphoreConfig config = nodeEngine.getConfig().getSemaphoreConfig(name);
            int partitionId = nodeEngine.getPartitionService().getPartitionId(nodeEngine.toData(name));
            permit = new Permit(partitionId, new SemaphoreConfig(config));
            Permit current = permitMap.putIfAbsent(name, permit);
            permit = current == null ? permit : current;
        }
        return permit;
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void destroy() {
        permitMap.clear();
    }

    public void memberAdded(MembershipServiceEvent event) {
    }

    public void memberRemoved(MembershipServiceEvent event) {
        System.out.println("removed: " + event);

        Address caller = event.getMember().getAddress();
        for (String name: permitMap.keySet()){
            int partitionId = nodeEngine.getPartitionService().getPartitionId(name);
            PartitionInfo info = nodeEngine.getPartitionService().getPartitionInfo(partitionId);
            if (nodeEngine.getThisAddress().equals(info.getOwner())){
                Operation op = new DeadMemberOperation(name, caller).setPartitionId(partitionId)
                        .setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler())
                        .setService(this).setNodeEngine(nodeEngine).setServiceName(SERVICE_NAME);
                nodeEngine.getOperationService().runOperation(op);
            }
        }
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        return new SemaphoreProxy((String)objectId, this, nodeEngine);
    }

    public DistributedObject createDistributedObjectForClient(Object objectId) {
        return createDistributedObject(objectId);
    }

    public void destroyDistributedObject(Object objectId) {
        permitMap.remove(String.valueOf(objectId));
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        Map<String, Permit> migrationData = new HashMap<String, Permit>();
        for (Map.Entry<String, Permit> entry: permitMap.entrySet()){
            String name = entry.getKey();
            Permit permit = entry.getValue();
            if (permit.getPartitionId() == event.getPartitionId() && permit.getConfig().getTotalBackupCount() >= event.getReplicaIndex()){
                migrationData.put(name, permit);
            }
        }
        if (migrationData.isEmpty()){
            return null;
        }
        return new SemaphoreMigrationOperation(migrationData);
    }

    public void insertMigrationData(Map<String, Permit> migrationData){
        permitMap.putAll(migrationData);
    }

    public void commitMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE){
            if (event.getMigrationType() == MigrationType.MOVE || event.getMigrationType() == MigrationType.MOVE_COPY_BACK){
                clearMigrationData(event.getPartitionId(), event.getCopyBackReplicaIndex());
            }
        }
    }

    private void clearMigrationData(int partitionId, int copyBack){
        Iterator<Map.Entry<String, Permit>> iter = permitMap.entrySet().iterator();
        while (iter.hasNext()){
            Permit permit = iter.next().getValue();
            if (permit.getPartitionId() == partitionId && (copyBack == -1 || permit.getConfig().getTotalBackupCount() < copyBack)){
                iter.remove();
            }
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId(), -1);
        }
    }

    public int getMaxBackupCount() {
        int max = 0;
        for (String name: permitMap.keySet()){
            SemaphoreConfig config = nodeEngine.getConfig().getSemaphoreConfig(name);
            max = Math.max(max,  config.getTotalBackupCount());
        }
        return max;
    }

    public ConcurrentMap<String, Permit> getPermitMap() {
        return permitMap; //TODO testing only
    }
}

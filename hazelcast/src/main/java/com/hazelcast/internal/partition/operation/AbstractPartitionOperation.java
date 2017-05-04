/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.InternalReplicaFragmentNamespace;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ReplicaFragmentNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Collections.singleton;

abstract class AbstractPartitionOperation extends Operation implements IdentifiedDataSerializable {

    final Collection<MigrationAwareService> getMigrationAwareServices() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        return nodeEngine.getServices(MigrationAwareService.class);
    }

    final Collection<Operation> createAllReplicationOperations(PartitionReplicationEvent event) {
        return createReplicationOperations(event, false);
    }

    final Collection<Operation> createNonFragmentedReplicationOperations(PartitionReplicationEvent event) {
        return createReplicationOperations(event, true);
    }

    private Collection<Operation> createReplicationOperations(PartitionReplicationEvent event, boolean nonFragmentedOnly) {
        Collection<Operation> operations = new ArrayList<Operation>();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = serviceInfo.getService();
            if (nonFragmentedOnly && service instanceof FragmentedMigrationAwareService) {
                // skip fragmented services
                continue;
            }

            Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                operations.add(op);
            }
        }
        return operations;
    }

    final Operation createFragmentReplicationOperation(PartitionReplicationEvent event, ReplicaFragmentNamespace ns) {
        assert !(ns instanceof InternalReplicaFragmentNamespace) : ns + " should be used only for non-fragmented services!";
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        FragmentedMigrationAwareService service = nodeEngine.getService(ns.getServiceName());
        Operation op = service.prepareReplicationOperation(event, singleton(ns));
        if (op != null) {
            op.setServiceName(ns.getServiceName());
        }
        return op;
    }

    @Override
    public final int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }
}

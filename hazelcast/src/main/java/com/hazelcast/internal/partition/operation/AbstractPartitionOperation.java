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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Collections.emptySet;
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
        Collection<Operation> operations = new ArrayList<>();
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

    final Collection<Operation> createFragmentReplicationOperations(PartitionReplicationEvent event, ServiceNamespace ns,
            Collection<String> serviceNames) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for non-fragmented services!";

        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        for (String serviceName : serviceNames) {
            FragmentedMigrationAwareService service = nodeEngine.getService(serviceName);
            assert service.isKnownServiceNamespace(ns) : ns + " should be known by " + service;

            operations = prepareAndAppendReplicationOperation(event, ns, service, serviceName, operations);
        }

        return operations;
    }

    final Collection<Operation> createFragmentReplicationOperations(PartitionReplicationEvent event, ServiceNamespace ns) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for non-fragmented services!";

        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = serviceInfo.getService();
            if (!service.isKnownServiceNamespace(ns)) {
                continue;
            }

            operations = prepareAndAppendReplicationOperation(event, ns, service, serviceInfo.getName(), operations);
        }
        return operations;
    }

    private Collection<Operation> prepareAndAppendReplicationOperation(PartitionReplicationEvent event, ServiceNamespace ns,
            FragmentedMigrationAwareService service, String serviceName, Collection<Operation> operations) {

        Operation op = service.prepareReplicationOperation(event, singleton(ns));
        if (op == null) {
            return operations;
        }

        op.setServiceName(serviceName);

        if (operations.isEmpty()) {
            // generally a namespace belongs to a single service only
            operations = singleton(op);
        } else if (operations.size() == 1) {
            operations = new ArrayList<>(operations);
            operations.add(op);
        } else {
            operations.add(op);
        }
        return operations;
    }

    @Override
    public final int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }
}

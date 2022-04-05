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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;
import static java.util.Collections.singleton;

/**
 * A namespace is used to group single or multiple services'.
 * <p>
 * For instance, map-service uses same namespace with lock-service
 * and ring-buffer-service when it needs to use their functionality.
 *
 * <p>
 * To clarify the concept, this is a sketch of multiple
 * services which are sharing the same namespace
 *
 * <pre>
 *
 * serviceName-1    serviceName-2    serviceName-3
 * /---------\      /---------\      /---------\
 * |         |      |         |      |         |
 * |         |      |         |      |         |
 * |         |      |         |      |         |
 * /-------------------------------------------\
 * | A SHARED NAMESPACE OF MULTIPLE SERVICES   |
 * \-------------------------------------------/
 * |         |      |         |      |         |
 * |         |      |         |      |         |
 * |         |      |         |      |         |
 * \---------/      \---------/      \---------/
 * </pre>
 */
final class ServiceNamespacesContext {

    private final Iterator<ServiceNamespace> namespaceIterator;
    private final Set<ServiceNamespace> allNamespaces = new HashSet<>();
    private final Map<ServiceNamespace, Collection<String>> namespaceToServices = new HashMap<>();

    private ServiceNamespace currentNamespace;

    ServiceNamespacesContext(NodeEngineImpl nodeEngine, PartitionReplicationEvent event) {
        nodeEngine.forEachMatchingService(FragmentedMigrationAwareService.class, serviceInfo -> {
            // get all namespaces of a service
            Collection<ServiceNamespace> namespaces = getAllServiceNamespaces(serviceInfo, event);
            if (isNotEmpty(namespaces)) {
                // update collection of unique namespaces
                allNamespaces.addAll(namespaces);
                // map namespace to serviceName
                namespaces.forEach(ns -> mapNamespaceToService(ns, serviceInfo.getName()));
            }
        });

        // add a namespace to represent non-fragmented services
        allNamespaces.add(NonFragmentedServiceNamespace.INSTANCE);

        namespaceIterator = allNamespaces.iterator();
    }

    private void mapNamespaceToService(ServiceNamespace ns, String serviceName) {
        Collection<String> existingServiceNames = namespaceToServices.get(ns);
        if (existingServiceNames == null) {
            // generally a namespace belongs to a single service only
            namespaceToServices.put(ns, singleton(serviceName));
            return;
        }

        if (existingServiceNames.size() == 1) {
            existingServiceNames = new HashSet<>(existingServiceNames);
            namespaceToServices.put(ns, existingServiceNames);
        }

        existingServiceNames.add(serviceName);

    }

    private Collection<ServiceNamespace> getAllServiceNamespaces(ServiceInfo serviceInfo,
                                                                 PartitionReplicationEvent event) {
        return ((FragmentedMigrationAwareService) serviceInfo
                .getService()).getAllServiceNamespaces(event);
    }

    boolean hasNext() {
        return namespaceIterator.hasNext();
    }

    ServiceNamespace current() {
        return currentNamespace;
    }

    ServiceNamespace next() {
        currentNamespace = namespaceIterator.next();
        return currentNamespace;
    }

    Collection<String> getServiceNames(ServiceNamespace ns) {
        return namespaceToServices.get(ns);
    }

    Set<ServiceNamespace> getAllNamespaces() {
        return allNamespaces;
    }
}

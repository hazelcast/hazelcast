/*
 * Copyright (c) 2008 - 2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.nio.tcp.OperationPacketFilter;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.util.collection.IntHashSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.LEN;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static java.util.Collections.singletonList;

final class PacketFiltersUtil {

    private PacketFiltersUtil() {
    }

    static void resetPacketFiltersFrom(HazelcastInstance instance) {
        Node node = getNode(instance);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        cm.removeDroppingPacketFilter();
        cm.removeDelayingPacketFilter();
    }

    static void delayOperationsFrom(HazelcastInstance instance, int... opTypes) {
        Node node = getNode(instance);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        PacketFilter packetFilter = new EndpointAgnosticClusterOperationPacketFilter(node.getSerializationService(), opTypes);
        cm.setDelayingPacketFilter(packetFilter, 500, 5000);
    }

    static void delayOperationsBetween(HazelcastInstance from, HazelcastInstance to, int... opTypes) {
        Node node = getNode(from);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        List<Address> blacklist = singletonList(getAddress(to));
        PacketFilter packetFilter = new EndpointAwareClusterOperationPacketFilter(node.getSerializationService(), blacklist, opTypes);
        cm.setDelayingPacketFilter(packetFilter, 500, 5000);
    }

    static void delayOperationsBetween(HazelcastInstance from, Collection<HazelcastInstance> to, int... opTypes) {
        Node node = getNode(from);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        Collection<Address> blacklist = getAddresses(to);
        PacketFilter packetFilter = new EndpointAwareClusterOperationPacketFilter(node.getSerializationService(), blacklist, opTypes);
        cm.setDelayingPacketFilter(packetFilter, 500, 5000);
    }

    static void dropOperationsFrom(HazelcastInstance instance, int... opTypes) {
        Node node = getNode(instance);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        PacketFilter packetFilter = new EndpointAgnosticClusterOperationPacketFilter(node.getSerializationService(), opTypes);
        cm.setDroppingPacketFilter(packetFilter);
    }

    static void dropOperationsBetween(HazelcastInstance from, HazelcastInstance to, int... opTypes) {
        Node node = getNode(from);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        List<Address> blacklist = singletonList(getAddress(to));
        PacketFilter packetFilter = new EndpointAwareClusterOperationPacketFilter(node.getSerializationService(), blacklist, opTypes);
        cm.setDroppingPacketFilter(packetFilter);
    }

    static void dropOperationsBetween(HazelcastInstance from, Collection<HazelcastInstance> to, int... opTypes) {
        Node node = getNode(from);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        Collection<Address> blacklist = getAddresses(to);
        PacketFilter packetFilter = new EndpointAwareClusterOperationPacketFilter(node.getSerializationService(), blacklist, opTypes);
        cm.setDroppingPacketFilter(packetFilter);
    }

    private static Collection<Address> getAddresses(Collection<HazelcastInstance> instances) {
        List<Address> addresses = new ArrayList<Address>();
        for (HazelcastInstance instance : instances) {
            addresses.add(getAddress(instance));
        }

        return addresses;
    }

    static class EndpointAgnosticClusterOperationPacketFilter extends OperationPacketFilter {

        final IntHashSet types = new IntHashSet(LEN, 0);

        EndpointAgnosticClusterOperationPacketFilter(InternalSerializationService serializationService, int...typeIds) {
            super(serializationService);
            
            assert typeIds.length > 0 : "At least one operation type must be defined!";
            for (int id : typeIds) {
                types.add(id);
            }
        }

        @Override
        protected boolean allowOperation(Address endpoint, int factory, int type) {
            boolean drop = factory == F_ID && types.contains(type);
            return !drop;
        }
    }

    static class EndpointAwareClusterOperationPacketFilter extends EndpointAgnosticClusterOperationPacketFilter {

        final Set<Address> blacklist = new HashSet<Address>();

        EndpointAwareClusterOperationPacketFilter(InternalSerializationService serializationService,
                                                  Collection<Address> blacklist,
                                                  int...typeIds) {
            super(serializationService, typeIds);
            this.blacklist.addAll(blacklist);
        }

        @Override
        protected boolean allowOperation(Address endpoint, int factory, int type) {
            return super.allowOperation(endpoint, factory, type) || !blacklist.contains(endpoint);
        }
    }

}

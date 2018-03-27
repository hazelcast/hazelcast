/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.nio.tcp.OperationPacketFilter;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.collection.IntHashSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static java.util.Collections.singleton;

@SuppressWarnings("unused")
public final class PacketFiltersUtil {

    private PacketFiltersUtil() {
    }

    public static void resetPacketFiltersFrom(HazelcastInstance instance) {
        Node node = getNode(instance);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        cm.removePacketFilter();
    }

    public static void delayOperationsFrom(HazelcastInstance instance, int factory, List<Integer> opTypes) {
       filterOperationsFrom(instance, factory, opTypes, PacketFilter.Action.DELAY);
    }

    public static void delayOperationsBetween(HazelcastInstance from, HazelcastInstance to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, singleton(to), factory, opTypes, PacketFilter.Action.DELAY);
    }

    public static void delayOperationsBetween(HazelcastInstance from, Collection<HazelcastInstance> to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, to, factory, opTypes, PacketFilter.Action.DELAY);
    }

    public static void dropOperationsFrom(HazelcastInstance instance, int factory, List<Integer> opTypes) {
        filterOperationsFrom(instance, factory, opTypes, PacketFilter.Action.DROP);
    }

    public static void dropOperationsBetween(HazelcastInstance from, HazelcastInstance to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, singleton(to), factory, opTypes, PacketFilter.Action.DROP);
    }

    public static void dropOperationsBetween(HazelcastInstance from, Collection<HazelcastInstance> to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, to, factory, opTypes, PacketFilter.Action.DROP);
    }

    public static void rejectOperationsFrom(HazelcastInstance instance, int factory, List<Integer> opTypes) {
        filterOperationsFrom(instance, factory, opTypes, PacketFilter.Action.REJECT);
    }

    public static void rejectOperationsBetween(HazelcastInstance from, HazelcastInstance to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, singleton(to), factory, opTypes, PacketFilter.Action.REJECT);
    }

    public static void rejectOperationsBetween(HazelcastInstance from, Collection<HazelcastInstance> to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, to, factory, opTypes, PacketFilter.Action.REJECT);
    }

    private static void filterOperationsFrom(HazelcastInstance instance, int factory, List<Integer> opTypes,
            PacketFilter.Action action) {
        Node node = getNode(instance);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        PacketFilter packetFilter = new EndpointAgnosticPacketFilter(node.getSerializationService(), factory, opTypes, action);
        cm.setPacketFilter(packetFilter);
    }

    private static void filterOperationsBetween(HazelcastInstance from, Collection<HazelcastInstance> to, int factory,
            List<Integer> opTypes, PacketFilter.Action action) {
        Node node = getNode(from);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        Collection<Address> blacklist = getAddresses(to);
        PacketFilter packetFilter = new EndpointAwarePacketFilter(node.getSerializationService(), blacklist, factory,
                opTypes, action);
        cm.setPacketFilter(packetFilter);
    }

    private static Collection<Address> getAddresses(Collection<HazelcastInstance> instances) {
        List<Address> addresses = new ArrayList<Address>();
        for (HazelcastInstance instance : instances) {
            addresses.add(getAddress(instance));
        }

        return addresses;
    }

    private static class EndpointAgnosticPacketFilter extends OperationPacketFilter {

        final int factory;
        final Action action;

        // Integer.MIN_VALUE is used for missing value
        final IntHashSet types = new IntHashSet(1024, Integer.MIN_VALUE);

        EndpointAgnosticPacketFilter(InternalSerializationService serializationService, int factory,
                List<Integer> typeIds, Action action) {
            super(serializationService);
            this.action = Preconditions.checkNotNull(action);
            assert typeIds.size() > 0 : "At least one operation type must be defined!";
            this.factory = factory;
            types.addAll(typeIds);
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            return (this.factory == factory && types.contains(type)) ? action : Action.ALLOW;
        }
    }

    private static class EndpointAwarePacketFilter extends EndpointAgnosticPacketFilter {

        final Set<Address> blacklist = new HashSet<Address>();

        EndpointAwarePacketFilter(InternalSerializationService serializationService, Collection<Address> blacklist, int factory,
                                  List<Integer> typeIds, Action action) {
            super(serializationService, factory, typeIds, action);
            this.blacklist.addAll(blacklist);
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            if (blacklist.contains(endpoint)) {
                return super.filterOperation(endpoint, factory, type);
            }
            return Action.ALLOW;
        }
    }
}

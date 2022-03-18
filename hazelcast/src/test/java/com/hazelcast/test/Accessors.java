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
package com.hazelcast.test;

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionReplicaStateChecker;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.starter.HazelcastStarter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

/**
 * Utility accessors for Hazelcast internals
 */
public class Accessors {

    private Accessors() {
    }

    public static Node getNode(HazelcastInstance hz) {
        return TestUtil.getNode(hz);
    }

    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        return TestUtil.getHazelcastInstanceImpl(hz);
    }

    public static NodeEngineImpl getNodeEngineImpl(HazelcastInstance hz) {
        return getNode(hz).getNodeEngine();
    }

    public static ClientEngineImpl getClientEngineImpl(HazelcastInstance instance) {
        return (ClientEngineImpl) getNode(instance).getClientEngine();
    }

    public static ServerConnectionManager getConnectionManager(HazelcastInstance hz) {
        return getNode(hz).getServer().getConnectionManager(EndpointQualifier.MEMBER);
    }

    public static ClusterService getClusterService(HazelcastInstance hz) {
        return getNode(hz).getClusterService();
    }

    public static InternalPartitionService getPartitionService(HazelcastInstance hz) {
        return getNode(hz).getPartitionService();
    }

    public static boolean isPartitionStateInitialized(HazelcastInstance hz) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(hz);
        return partitionService.getPartitionStateManager().isInitialized();
    }

    public static InternalSerializationService getSerializationService(HazelcastInstance hz) {
        return getNode(hz).getSerializationService();
    }

    public static OperationServiceImpl getOperationService(HazelcastInstance hz) {
        return getNodeEngineImpl(hz).getOperationService();
    }

    public static MetricsRegistry getMetricsRegistry(HazelcastInstance hz) {
        return getNodeEngineImpl(hz).getMetricsRegistry();
    }

    public static AuditlogService getAuditlogService(HazelcastInstance hz) {
        return getNode(hz).getNodeExtension().getAuditlogService();
    }

    public static <T> T getService(HazelcastInstance hz, String serviceName) {
        return getNodeEngineImpl(hz).getService(serviceName);
    }

    public static Address getAddress(HazelcastInstance hz) {
        return getClusterService(hz).getThisAddress();
    }

    public static Address[] getAddresses(HazelcastInstance[] instances) {
        Address[] addresses = new Address[instances.length];
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = getAddress(instances[i]);
        }
        return addresses;
    }

    public static Address getAddress(HazelcastInstance hz, EndpointQualifier qualifier) {
        return new Address(getClusterService(hz).getLocalMember().getSocketAddress(qualifier));
    }

    /**
     * Returns the partition ID from a non-partitioned Hazelcast data
     * structures like {@link ISet}.
     * <p>
     * The partition ID is read via reflection from the internal
     * {@code partitionId} field. This is needed to support proxied
     * Hazelcast instances from {@link HazelcastStarter}.
     *
     * @param hazelcastDataStructure the Hazelcast data structure instance
     * @return the partition ID of that data structure
     */
    public static int getPartitionIdViaReflection(Object hazelcastDataStructure) {
        try {
            return (Integer) getFieldValueReflectively(hazelcastDataStructure, "partitionId");
        } catch (IllegalAccessException e) {
            throw new AssertionError("Cannot retrieve partitionId field from class "
                    + hazelcastDataStructure.getClass().getName());
        }
    }

    public static HazelcastInstance getFirstBackupInstance(HazelcastInstance[] instances, int partitionId) {
        return getBackupInstance(instances, partitionId, 1);
    }

    public static HazelcastInstance getBackupInstance(HazelcastInstance[] instances, int partitionId, int replicaIndex) {
        InternalPartition partition = getPartitionService(instances[0]).getPartition(partitionId);
        Address backupAddress = partition.getReplicaAddress(replicaIndex);
        for (HazelcastInstance instance : instances) {
            if (instance.getCluster().getLocalMember().getAddress().equals(backupAddress)) {
                return instance;
            }
        }
        throw new AssertionError("Could not find backup member for partition " + partitionId);
    }

    /**
     * Obtains a list of {@link Indexes} for the given map local to the node
     * associated with it.
     * <p>
     * There may be more than one indexes instance associated with a map if its
     * indexes are partitioned.
     *
     * @param map the map to obtain the indexes for.
     * @return the obtained indexes list.
     */
    public static List<Indexes> getAllIndexes(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        String mapName = mapProxy.getName();
        NodeEngine nodeEngine = mapProxy.getNodeEngine();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);

        Indexes maybeGlobalIndexes = mapContainer.getIndexes();
        if (maybeGlobalIndexes != null) {
            return Collections.singletonList(maybeGlobalIndexes);
        }

        PartitionContainer[] partitionContainers = mapServiceContext.getPartitionContainers();
        List<Indexes> allIndexes = new ArrayList<>();
        for (PartitionContainer partitionContainer : partitionContainers) {
            IPartition partition = partitionService.getPartition(partitionContainer.getPartitionId());
            if (!partition.isLocal()) {
                continue;
            }

            Indexes partitionIndexes = partitionContainer.getIndexes().get(mapName);
            if (partitionIndexes == null) {
                continue;
            }
            assert !partitionIndexes.isGlobal();
            allIndexes.add(partitionIndexes);
        }

        return allIndexes;
    }

    public static PartitionReplicaStateChecker getPartitionReplicaStateChecker(HazelcastInstance instance) {
        return getPartitionService(instance).getPartitionReplicaStateChecker();
    }
}

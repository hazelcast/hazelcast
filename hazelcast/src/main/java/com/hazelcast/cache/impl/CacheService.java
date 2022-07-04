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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Collection;

import static com.hazelcast.internal.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.internal.partition.MigrationEndpoint.SOURCE;

/**
 * Cache Service is the main access point of JCache implementation.
 * <p>
 * This service is responsible for:
 * <ul>
 * <li>Creating and/or accessing the named {@link com.hazelcast.cache.impl.CacheRecordStore}.</li>
 * <li>Creating/Deleting the cache configuration of the named {@link com.hazelcast.cache.ICache}.</li>
 * <li>Registering/Deregistering of cache listeners.</li>
 * <li>Publish/dispatch cache events.</li>
 * <li>Enabling/Disabling statistic and management.</li>
 * <li>Data migration commit/rollback through {@link MigrationAwareService}.</li>
 * </ul>
 * <p><b>WARNING:</b>This service is an optionally registered service which is enabled when JCache
 * is located on the classpath, as determined by {@link JCacheDetector#isJCacheAvailable(ClassLoader)}.</p>
 * <p>
 * If registered, it will provide all the above cache operations for all partitions of the node which it
 * is registered on.
 * </p>
 * <p><b>Distributed Cache Name</b> is used for providing a unique name to a cache object to overcome cache manager
 * scoping which depends on URI and class loader parameters. It's a simple concatenation of CacheNamePrefix and
 * cache name where CacheNamePrefix is calculated by each cache manager
 * using {@link AbstractHazelcastCacheManager#getCacheNamePrefix()}.
 * </p>
 */
public class CacheService extends AbstractCacheService {

    @Override
    protected CachePartitionSegment newPartitionSegment(int partitionId) {
        return new CachePartitionSegment(this, partitionId);
    }

    @Override
    protected ICacheRecordStore createNewRecordStore(String cacheNameWithPrefix, int partitionId) {
        CacheRecordStore recordStore = new CacheRecordStore(cacheNameWithPrefix, partitionId, nodeEngine, this);
        recordStore.instrument(nodeEngine);
        return recordStore;
    }

    @Override
    protected CacheOperationProvider createOperationProvider(String nameWithPrefix, InMemoryFormat inMemoryFormat) {
        return new DefaultOperationProvider(nameWithPrefix);
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        return segment.getAllNamespaces(event.getReplicaIndex());
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace && SERVICE_NAME.equals(namespace.getServiceName());
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        return prepareReplicationOperation(event, segment.getAllNamespaces(event.getReplicaIndex()));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        assert assertAllKnownNamespaces(namespaces);

        CachePartitionSegment segment = segments[event.getPartitionId()];
        CacheReplicationOperation op = newCacheReplicationOperation();
        op.setPartitionId(event.getPartitionId());
        op.prepare(segment, namespaces, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    protected boolean assertAllKnownNamespaces(Collection<ServiceNamespace> namespaces) {
        for (ServiceNamespace namespace : namespaces) {
            assert isKnownServiceNamespace(namespace) : namespace + " is not a CacheService namespace!";
        }
        return true;
    }

    protected CacheReplicationOperation newCacheReplicationOperation() {
        return new CacheReplicationOperation();
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        super.commitMigration(event);

        if (SOURCE == event.getMigrationEndpoint()) {
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
        } else if (DESTINATION == event.getMigrationEndpoint()) {
            if (event.getNewReplicaIndex() != 0) {
                getMetaDataGenerator().regenerateUuid(event.getPartitionId());
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        super.rollbackMigration(event);

        if (DESTINATION == event.getMigrationEndpoint()) {
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
        }
    }

    private MetaDataGenerator getMetaDataGenerator() {
        return cacheEventHandler.getMetaDataGenerator();
    }

    @Override
    public String toString() {
        return "CacheService[" + SERVICE_NAME + ']';
    }

    @Override
    public boolean isWanReplicationEnabled(String cacheNameWithPrefix) {
        return false;
    }

    @Override
    public CacheWanEventPublisher getCacheWanEventPublisher() {
        throw new UnsupportedOperationException("WAN replication is not supported");
    }

    @Override
    public void doPrepublicationChecks(String cacheName) {
        // NOP intentionally
    }

    public static ObjectNamespace getObjectNamespace(String cacheName) {
        return new DistributedObjectNamespace(SERVICE_NAME, cacheName);
    }
}

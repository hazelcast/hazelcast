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

package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.journal.CacheEventJournal;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.journal.MapEventJournal;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.REPLICATION_OPERATION;
import static com.hazelcast.ringbuffer.impl.RingbufferService.SERVICE_NAME;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class ReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private Map<ObjectNamespace, RingbufferContainer> migrationData;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<ObjectNamespace, RingbufferContainer> migrationData,
                                int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() {
        final RingbufferService service = getService();
        for (Map.Entry<ObjectNamespace, RingbufferContainer> entry : migrationData.entrySet()) {
            final ObjectNamespace ns = entry.getKey();
            final RingbufferContainer ringbuffer = entry.getValue();
            service.addRingbuffer(getPartitionId(), ringbuffer, getRingbufferConfig(service, ns));
        }
    }

    /**
     * Returns the ringbuffer config for the provided namespace. The namespace
     * provides information whether the requested ringbuffer is a ringbuffer
     * that the user is directly interacting with through a ringbuffer proxy
     * or if this is a backing ringbuffer for an event journal.
     * If a ringbuffer configuration for an event journal is requested, this
     * method will expect the configuration for the relevant map or cache
     * to be available.
     *
     * @param service the ringbuffer service
     * @param ns      the object namespace for which we are creating a ringbuffer
     * @return the ringbuffer configuration
     * @throws CacheNotExistsException if a config for a cache event journal was requested
     *                                 and the cache configuration was not found
     */
    private RingbufferConfig getRingbufferConfig(RingbufferService service, ObjectNamespace ns) {
        final String serviceName = ns.getServiceName();
        if (RingbufferService.SERVICE_NAME.equals(serviceName)) {
            return service.getRingbufferConfig(ns.getObjectName());
        } else if (MapService.SERVICE_NAME.equals(serviceName)) {
            final MapService mapService = getNodeEngine().getService(MapService.SERVICE_NAME);
            final MapEventJournal journal = mapService.getMapServiceContext().getEventJournal();
            final EventJournalConfig journalConfig = journal.getEventJournalConfig(ns);
            return journal.toRingbufferConfig(journalConfig, ns);
        } else if (CacheService.SERVICE_NAME.equals(serviceName)) {
            final CacheService cacheService = getNodeEngine().getService(CacheService.SERVICE_NAME);
            final CacheEventJournal journal = cacheService.getEventJournal();
            final EventJournalConfig journalConfig = journal.getEventJournalConfig(ns);
            return journal.toRingbufferConfig(journalConfig, ns);
        } else {
            throw new IllegalArgumentException("Unsupported ringbuffer service name " + serviceName);
        }
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getClassId() {
        return REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Entry<ObjectNamespace, RingbufferContainer> entry : migrationData.entrySet()) {
            final ObjectNamespace ns = entry.getKey();
            out.writeObject(ns);
            RingbufferContainer container = entry.getValue();
            container.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = createHashMap(mapSize);
        for (int i = 0; i < mapSize; i++) {
            final ObjectNamespace namespace = in.readObject();
            final RingbufferContainer container = new RingbufferContainer(namespace, getPartitionId());
            container.readData(in);
            migrationData.put(namespace, container);
        }
    }
}

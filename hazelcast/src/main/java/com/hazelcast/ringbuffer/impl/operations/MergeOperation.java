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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.MergingValueHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.MERGE_OPERATION;
import static com.hazelcast.ringbuffer.impl.RingbufferService.SERVICE_NAME;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merge entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends Operation implements IdentifiedDataSerializable, BackupAwareOperation, ServiceNamespaceAware {

    private ObjectNamespace namespace;
    private SplitBrainMergePolicy mergePolicy;
    private List<MergingValueHolder<Object>> mergingValues;

    private transient RingbufferConfig config;
    private transient RingbufferContainer<Object, Object> ringbuffer;
    private transient Map<Long, Data> valueMap;

    public MergeOperation() {
    }

    public MergeOperation(ObjectNamespace namespace, SplitBrainMergePolicy mergePolicy,
                          List<MergingValueHolder<Object>> mergingValues) {
        this.namespace = namespace;
        this.mergePolicy = mergePolicy;
        this.mergingValues = mergingValues;
    }

    @Override
    public void beforeRun() throws Exception {
        RingbufferService service = getService();
        config = getRingbufferConfig(service, namespace);
        ringbuffer = service.getOrCreateContainer(getPartitionId(), namespace, config);
    }

    @Override
    public void run() throws Exception {
        valueMap = createHashMap(mergingValues.size());
        for (MergingValueHolder<Object> mergingValue : mergingValues) {
            long resultSequence = ringbuffer.merge(mergingValue, mergePolicy);
            if (resultSequence != -1) {
                valueMap.put(resultSequence, ringbuffer.readAsData(resultSequence));
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return valueMap != null && !valueMap.isEmpty();
    }

    @Override
    public int getSyncBackupCount() {
        return config.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return config.getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new MergeBackupOperation(namespace.getObjectName(), valueMap);
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
            MapService mapService = getNodeEngine().getService(MapService.SERVICE_NAME);
            MapEventJournal journal = mapService.getMapServiceContext().getEventJournal();
            EventJournalConfig journalConfig = journal.getEventJournalConfig(ns);
            return journal.toRingbufferConfig(journalConfig);
        } else if (CacheService.SERVICE_NAME.equals(serviceName)) {
            CacheService cacheService = getNodeEngine().getService(CacheService.SERVICE_NAME);
            CacheEventJournal journal = cacheService.getEventJournal();
            EventJournalConfig journalConfig = journal.getEventJournalConfig(ns);
            return journal.toRingbufferConfig(journalConfig);
        } else {
            throw new IllegalArgumentException("Unsupported ringbuffer service name: " + serviceName);
        }
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        return namespace;
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
    public int getId() {
        return MERGE_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(namespace);
        out.writeObject(mergePolicy);
        out.writeInt(mergingValues.size());
        for (MergingValueHolder<Object> mergingValue : mergingValues) {
            out.writeObject(mergingValue);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        namespace = in.readObject();
        mergePolicy = in.readObject();
        int size = in.readInt();
        mergingValues = new ArrayList<MergingValueHolder<Object>>(size);
        for (int i = 0; i < size; i++) {
            MergingValueHolder<Object> mergingValue = in.readObject();
            mergingValues.add(mergingValue);
        }
    }
}

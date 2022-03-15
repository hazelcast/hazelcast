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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.ringbuffer.impl.ArrayRingbuffer;
import com.hazelcast.ringbuffer.impl.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.spi.merge.RingbufferMergeData;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;

import static com.hazelcast.internal.nio.IOUtil.readObject;
import static com.hazelcast.internal.nio.IOUtil.writeObject;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.MERGE_OPERATION;
import static com.hazelcast.ringbuffer.impl.RingbufferService.SERVICE_NAME;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingValue;

/**
 * Contains an entire ringbuffer for split-brain healing with a
 * {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends Operation
        implements IdentifiedDataSerializable, BackupAwareOperation, ServiceNamespaceAware {

    private ObjectNamespace namespace;
    private SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes, RingbufferMergeData> mergePolicy;
    private Ringbuffer<Object> mergingRingbuffer;

    private transient Ringbuffer<Object> resultRingbuffer;
    private transient RingbufferConfig config;
    private transient RingbufferService ringbufferService;
    private transient SerializationService serializationService;

    public MergeOperation() {
    }

    public MergeOperation(ObjectNamespace namespace,
                          SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes, RingbufferMergeData> mergePolicy,
                          Ringbuffer<Object> mergingRingbuffer) {
        this.namespace = namespace;
        this.mergePolicy = mergePolicy;
        this.mergingRingbuffer = mergingRingbuffer;
    }

    @Override
    public void beforeRun() throws Exception {
        this.ringbufferService = getService();
        this.config = getRingbufferConfig(ringbufferService, namespace);
        this.serializationService = getNodeEngine().getSerializationService();
    }

    @Override
    public void run() throws Exception {
        RingbufferContainer<Object, Object> existingContainer
                = ringbufferService.getContainerOrNull(getPartitionId(), namespace);

        RingbufferMergeTypes mergingValue =
                createMergingValue(serializationService, mergingRingbuffer);
        mergePolicy = (SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes, RingbufferMergeData>)
            serializationService.getManagedContext().initialize(mergePolicy);

        resultRingbuffer = merge(existingContainer, mergingValue);
    }

    /**
     * Merges the provided {@code mergingValue} into the {@code existingContainer}
     * and returns the merged ringbuffer.
     *
     * @param existingContainer the container into which to merge the data
     * @param mergingValue      the data to merge
     * @return the merged ringbuffer
     */
    private Ringbuffer<Object> merge(RingbufferContainer<Object, Object> existingContainer, RingbufferMergeTypes mergingValue) {
        RingbufferMergeTypes existingValue = createMergingValueOrNull(existingContainer);

        RingbufferMergeData resultData = mergePolicy.merge(mergingValue, existingValue);

        if (resultData == null) {
            ringbufferService.destroyDistributedObject(namespace.getObjectName());
            return null;
        } else {
            if (existingContainer == null) {
                RingbufferConfig config = getRingbufferConfig(ringbufferService, namespace);
                existingContainer = ringbufferService.getOrCreateContainer(getPartitionId(), namespace, config);
            }
            setRingbufferData(resultData, existingContainer);
            return existingContainer.getRingbuffer();
        }
    }

    private RingbufferMergeTypes createMergingValueOrNull(RingbufferContainer<Object, Object> existingContainer) {
        return existingContainer == null || existingContainer.getRingbuffer().isEmpty()
                ? null
                : createMergingValue(serializationService, existingContainer.getRingbuffer());
    }

    /**
     * Sets the ringbuffer data given by the {@code fromMergeData} to the
     * {@code toContainer}.
     *
     * @param fromMergeData the data which needs to be set into the containter
     * @param toContainer   the target ringbuffer container
     */
    private void setRingbufferData(
            RingbufferMergeData fromMergeData,
            RingbufferContainer<Object, Object> toContainer) {
        boolean storeEnabled = toContainer.getStore().isEnabled();
        Data[] storeItems = storeEnabled ? new Data[fromMergeData.size()] : null;

        toContainer.setHeadSequence(fromMergeData.getHeadSequence());
        toContainer.setTailSequence(fromMergeData.getTailSequence());

        for (long seq = fromMergeData.getHeadSequence(); seq <= fromMergeData.getTailSequence(); seq++) {
            final Object resultValue = fromMergeData.read(seq);
            toContainer.set(seq, resultValue);
            if (storeEnabled) {
                storeItems[(int) (seq - fromMergeData.getHeadSequence())] = serializationService.toData(resultValue);
            }
        }
        if (storeEnabled) {
            toContainer.getStore()
                    .storeAll(fromMergeData.getHeadSequence(), storeItems);
        }
    }

    @Override
    public boolean shouldBackup() {
        return true;
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
        return new MergeBackupOperation(namespace.getObjectName(), resultRingbuffer);
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
            return journal.toRingbufferConfig(journalConfig, namespace);
        } else if (CacheService.SERVICE_NAME.equals(serviceName)) {
            CacheService cacheService = getNodeEngine().getService(CacheService.SERVICE_NAME);
            CacheEventJournal journal = cacheService.getEventJournal();
            EventJournalConfig journalConfig = journal.getEventJournalConfig(ns);
            return journal.toRingbufferConfig(journalConfig, namespace);
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
    public int getClassId() {
        return MERGE_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(namespace);
        out.writeObject(mergePolicy);

        out.writeLong(mergingRingbuffer.tailSequence());
        out.writeLong(mergingRingbuffer.headSequence());
        out.writeInt((int) mergingRingbuffer.getCapacity());
        for (Object mergingItem : mergingRingbuffer) {
            writeObject(out, mergingItem);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        namespace = in.readObject();
        mergePolicy = in.readObject();

        final long tailSequence = in.readLong();
        final long headSequence = in.readLong();
        final int capacity = in.readInt();
        mergingRingbuffer = new ArrayRingbuffer<Object>(capacity);
        mergingRingbuffer.setTailSequence(tailSequence);
        mergingRingbuffer.setHeadSequence(headSequence);

        for (long seq = headSequence; seq <= tailSequence; seq++) {
            mergingRingbuffer.set(seq, readObject(in));
        }
    }
}

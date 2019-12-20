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

package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventService;
import com.hazelcast.wan.WanEventType;
import com.hazelcast.wan.impl.InternalWanEvent;
import com.hazelcast.wan.impl.WanDataSerializerHook;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * WAN replication object for map update operations.
 */
public class WanMapAddOrUpdateEvent implements InternalWanEvent<EntryView<Object, Object>>,
        IdentifiedDataSerializable, SerializationServiceAware {
    private String mapName;
    /**
     * The policy how to merge the entry on the receiving cluster
     */
    private SplitBrainMergePolicy mergePolicy;
    /**
     * The updated entry
     */
    private WanMapEntryView<Data, Data> entryView;
    private SerializationService serializationService;

    public WanMapAddOrUpdateEvent() {
    }

    public WanMapAddOrUpdateEvent(String mapName,
                                  SplitBrainMergePolicy mergePolicy,
                                  EntryView<Data, Data> entryView,
                                  SerializationService serializationService) {
        this.mergePolicy = mergePolicy;
        this.mapName = mapName;
        this.serializationService = serializationService;
        if (entryView instanceof WanMapEntryView) {
            this.entryView = (WanMapEntryView<Data, Data>) entryView;
        } else {
            this.entryView = new WanMapEntryView<>(entryView);
        }
    }

    public SplitBrainMergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public void setMergePolicy(SplitBrainMergePolicy mergePolicy) {
        this.mergePolicy = mergePolicy;
    }

    public WanMapEntryView<Data, Data> getEntryView() {
        return entryView;
    }

    public void setEntryView(WanMapEntryView<Data, Data> entryView) {
        this.entryView = entryView;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeObject(mergePolicy);
        out.writeObject(entryView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        mergePolicy = in.readObject();
        entryView = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.MAP_REPLICATION_UPDATE;
    }

    @Override
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementUpdate(mapName);
    }

    @Nonnull
    @Override
    public WanEventService getService() {
        return WanEventService.MAP;
    }

    @Nonnull
    @Override
    public Data getKey() {
        return entryView.getKey();
    }

    @Nonnull
    @Override
    public Set<String> getClusterNames() {
        // called only in EE
        return Collections.emptySet();
    }

    @Override
    public int getBackupCount() {
        // called only in EE
        return 0;
    }

    @Override
    public long getCreationTime() {
        // called only in EE
        return 0;
    }

    @Nonnull
    @Override
    public String getObjectName() {
        return mapName;
    }

    @Nonnull
    @Override
    public WanEventType getEventType() {
        return WanEventType.ADD_OR_UPDATE;
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public EntryView<Object, Object> getEventObject() {
        return EntryViews.toLazyEntryView((EntryView) entryView, serializationService);
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }
}

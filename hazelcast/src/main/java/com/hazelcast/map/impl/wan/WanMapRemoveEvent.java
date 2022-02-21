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

package com.hazelcast.map.impl.wan;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventType;
import com.hazelcast.wan.impl.InternalWanEvent;
import com.hazelcast.wan.impl.WanDataSerializerHook;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class WanMapRemoveEvent implements InternalWanEvent<Object>, IdentifiedDataSerializable, SerializationServiceAware {
    private SerializationService serializationService;
    private String mapName;
    private Data dataKey;
    private transient Object key;

    public WanMapRemoveEvent(@Nonnull String mapName,
                             @Nonnull Data dataKey,
                             @Nonnull SerializationService serializationService) {
        this.mapName = mapName;
        this.dataKey = dataKey;
        this.serializationService = serializationService;
    }

    public WanMapRemoveEvent() {
    }

    @Nonnull
    @Override
    public Data getKey() {
        return dataKey;
    }

    @Nullable
    @Override
    public Object getEventObject() {
        if (key == null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
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
        return WanEventType.REMOVE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        IOUtil.writeData(out, dataKey);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        dataKey = IOUtil.readData(in);
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.MAP_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementRemove(mapName);
    }

    @Nonnull
    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }
}

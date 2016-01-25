/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.util.Preconditions.checkNotNull;

public abstract class MultiMapOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;
    protected transient Object response;
    private transient MultiMapContainer container;

    protected MultiMapOperation() {
    }

    protected MultiMapOperation(String name) {
        this.name = name;
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    @Override
    public final String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    public final boolean hasListener() {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
        return registrations.size() > 0;
    }

    public final void publishEvent(EntryEventType eventType, Data key, Object newValue, Object oldValue) {
        MultiMapService multiMapService = getService();
        multiMapService.publishEntryEvent(name, eventType, key, newValue, oldValue);
    }

    public final Object toObject(Object obj) {
        return getNodeEngine().toObject(obj);
    }

    public final Data toData(Object obj) {
        return getNodeEngine().toData(obj);
    }

    public final MultiMapContainer getOrCreateContainer() {
        if (container == null) {
            MultiMapService service = getService();
            container = service.getOrCreateCollectionContainer(getPartitionId(), name);
        }
        return container;
    }

    public final MultiMapConfig.ValueCollectionType getValueCollectionType(MultiMapContainer container) {
        checkNotNull(container, "Argument container should not be null");

        MultiMapConfig config = container.getConfig();
        return config.getValueCollectionType();

    }

    public final boolean isBinary() {
        return getOrCreateContainer().getConfig().isBinary();
    }

    public final int getSyncBackupCount() {
        return getOrCreateContainer().getConfig().getSyncBackupCount();
    }

    public final int getAsyncBackupCount() {
        return getOrCreateContainer().getConfig().getAsyncBackupCount();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }
}

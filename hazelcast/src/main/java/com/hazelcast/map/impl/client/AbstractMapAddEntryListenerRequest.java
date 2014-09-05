/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.QueryEventFilter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.impl.PortableEntryEvent;

import java.security.Permission;

/**
 * Base class for adding entry listener to map
 */
public abstract class AbstractMapAddEntryListenerRequest extends CallableClientRequest
        implements RetryableRequest {
    protected String name;
    protected Data key;
    protected boolean includeValue;

    public AbstractMapAddEntryListenerRequest() {
    }

    public AbstractMapAddEntryListenerRequest(String name, boolean includeValue) {
        this.name = name;
        this.includeValue = includeValue;
    }

    public AbstractMapAddEntryListenerRequest(String name, Data key, boolean includeValue) {
        this(name, includeValue);
        this.key = key;
    }

    protected abstract Predicate getPredicate();

    @Override
    public Object call() {
        final ClientEndpoint endpoint = getEndpoint();
        final MapService mapService = getService();


        EntryListener<Object, Object> listener = new EntryAdapter<Object, Object>() {

            @Override
            public void onEntryEvent(EntryEvent<Object, Object> event) {
                if (endpoint.live()) {
                    if (!(event instanceof DataAwareEntryEvent)) {
                        throw new IllegalArgumentException("Expecting: DataAwareEntryEvent, Found: "
                                + event.getClass().getSimpleName());
                    }
                    DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
                    Data key = dataAwareEntryEvent.getKeyData();
                    Data value = dataAwareEntryEvent.getNewValueData();
                    Data oldValue = dataAwareEntryEvent.getOldValueData();
                    PortableEntryEvent portableEntryEvent = new PortableEntryEvent(key, value, oldValue,
                            event.getEventType(), event.getMember().getUuid());
                    endpoint.sendEvent(portableEntryEvent, getCallId());
                }
            }

            @Override
            public void onMapEvent(MapEvent event) {
                if (endpoint.live()) {
                    final EntryEventType type = event.getEventType();
                    final String uuid = event.getMember().getUuid();
                    PortableEntryEvent portableEntryEvent =
                            new PortableEntryEvent(type, uuid, event.getNumberOfEntriesAffected());
                    endpoint.sendEvent(portableEntryEvent, getCallId());
                }
            }
        };

        EventFilter eventFilter;
        if (getPredicate() == null) {
            eventFilter = new EntryEventFilter(includeValue, key);
        } else {
            eventFilter = new QueryEventFilter(includeValue, key, getPredicate());
        }
        String registrationId = mapService.getMapServiceContext().addEventListener(listener, eventFilter, name);
        endpoint.setListenerRegistration(MapService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}

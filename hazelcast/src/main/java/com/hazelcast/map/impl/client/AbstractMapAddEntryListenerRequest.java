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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.BaseClientAddListenerRequest;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.EventFilter;

import java.security.Permission;

/**
 * Base class for adding entry listener to map
 */
public abstract class AbstractMapAddEntryListenerRequest extends BaseClientAddListenerRequest {
    protected String name;
    protected Data key;
    protected boolean includeValue;
    protected int listenerFlags;

    public AbstractMapAddEntryListenerRequest() {
    }

    public AbstractMapAddEntryListenerRequest(String name, boolean includeValue, int listenerFlags) {
        this.name = name;
        this.includeValue = includeValue;
        this.listenerFlags = listenerFlags;
    }

    public AbstractMapAddEntryListenerRequest(String name, Data key, boolean includeValue, int listenerFlags) {
        this(name, includeValue, listenerFlags);
        this.key = key;
    }

    protected abstract Predicate getPredicate();

    @Override
    public Object call() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        ClientEndpoint endpoint = getEndpoint();
        Object listener = newMapListener(endpoint);

        EventFilter eventFilter = getEventFilter();
        EventFilter eventListenerFilter = new EventListenerFilter(listenerFlags, eventFilter);
        String registrationId;
        if (localOnly) {
            registrationId = mapServiceContext.addLocalEventListener(listener, eventListenerFilter, name);
        } else {
            registrationId = mapServiceContext.addEventListener(listener, eventListenerFilter, name);
        }
        endpoint.addListenerDestroyAction(MapService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    protected Object newMapListener(ClientEndpoint endpoint) {
        return new ClientMapListener(endpoint, getCallId());
    }

    protected EventFilter getEventFilter() {
        if (getPredicate() == null) {
            return new EntryEventFilter(includeValue, key);
        }
        return new QueryEventFilter(includeValue, key, getPredicate());
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

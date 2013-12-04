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

package com.hazelcast.map.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.map.EntryEventFilter;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.QueryEventFilter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.impl.PortableEntryEvent;

import java.security.Permission;

/**
 * User: sancar
 * Date: 11/11/13
 * Time: 11:08
 */
public abstract class AbstractMapAddEntryListenerRequest extends CallableClientRequest implements Portable, SecureRequest {
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
        final ClientEngine clientEngine = getClientEngine();
        final MapService mapService = getService();

        EntryListener<Object, Object> listener = new EntryListener<Object, Object>() {

            private void handleEvent(EntryEvent<Object, Object> event) {
                if (endpoint.live()) {
                    Data key = clientEngine.toData(event.getKey());
                    Data value = clientEngine.toData(event.getValue());
                    Data oldValue = clientEngine.toData(event.getOldValue());
                    PortableEntryEvent portableEntryEvent = new PortableEntryEvent(key, value, oldValue, event.getEventType(), event.getMember().getUuid());
                    clientEngine.sendResponse(endpoint, portableEntryEvent);
                }
            }

            public void entryAdded(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryRemoved(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryUpdated(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryEvicted(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }
        };

        EventFilter eventFilter;
        if (getPredicate() == null) {
            eventFilter = new EntryEventFilter(includeValue, key);
        } else {
            eventFilter = new QueryEventFilter(includeValue, key, getPredicate());
        }
        String registrationId = mapService.addEventListener(listener, eventFilter, name);
        endpoint.setListenerRegistration(MapService.SERVICE_NAME, name, registrationId);
        return true;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_LISTEN);
    }

}

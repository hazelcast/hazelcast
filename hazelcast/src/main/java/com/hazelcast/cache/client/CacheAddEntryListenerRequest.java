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

package com.hazelcast.cache.client;

import com.hazelcast.cache.CacheEventListener;
import com.hazelcast.cache.CachePortableHook;
import com.hazelcast.cache.CacheService;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.io.IOException;
import java.security.Permission;

public class CacheAddEntryListenerRequest
        extends CallableClientRequest
        implements RetryableRequest {

    private String name;
    private CacheEntryListenerConfiguration configuration;

    public CacheAddEntryListenerRequest() {
    }

    public CacheAddEntryListenerRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        ClientEndpoint endpoint = getEndpoint();
        CacheService cacheService = getService();
        //CacheInvalidationListener listener = new CacheInvalidationListener(endpoint, getCallId());
        //FIXME CLIENT LISTNER FIX
        CacheEventListener listener = null;
        String registrationId = cacheService.registerListener(name, listener);
        endpoint.setListenerRegistration(MapService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.ADD_ENTRY_LISTENER;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
        final ObjectDataOutput output = writer.getRawDataOutput();
        output.writeObject(configuration);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
        final ObjectDataInput input = reader.getRawDataInput();
        configuration = input.readObject();
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    //    class CacheEventListenerAdaptorWrapper<K,V> extends CacheEventListenerAdaptor<K,V>{
    //
    //        public CacheEventListenerAdaptorWrapper(CacheMeta cacheMeta, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    //            super(cacheMeta, cacheEntryListenerConfiguration);
    //        }
    //
    //        @Override
    //        public void handleEvent(EventType eventType, K key, V newValue, V oldValue, Class<K> keyClass, Class<V> valueClass) {
    ////            super.handleEvent(eventType, key, newValue, oldValue, keyClass, valueClass);
    //            if (endpoint.live()) {
    //                Data dataKey = serializationService.toData(key);
    //                Data dataNewValue = serializationService.toData(newValue);
    //                Data dataOldValue = serializationService.toData(oldValue);
    //                CacheEventDataImpl cacheEventData = new CacheEventDataImpl(cacheMeta.getDistributedObjectName(), dataKey,  dataNewValue,  dataOldValue,  eventType);
    //                endpoint.sendEvent(cacheEventData, getCallId());
    //            }
    //        }
    //    }
}

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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.io.IOException;

/**
 * <p>Listener can be registered and unregistered by {@link com.hazelcast.cache.impl.CacheService}. This operator
 * delegates the request to the cache service.</p>
 */
public class CacheListenerRegistrationOperation
        extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private CacheEntryListenerConfiguration cacheEntryListenerConfiguration;
    private boolean register;

    public CacheListenerRegistrationOperation() {
    }

    public CacheListenerRegistrationOperation(String name, CacheEntryListenerConfiguration cacheEntryListenerConfiguration,
                                              boolean register) {
        super(name);
        this.cacheEntryListenerConfiguration = cacheEntryListenerConfiguration;
        this.register = register;
    }

    @Override
    public void run()
            throws Exception {
        final CacheService service = getService();
        CacheConfig cacheConfig = service.getCacheConfig(name);
        if (register) {
            //REGISTER
            if (cacheConfig == null) {
                throw new IllegalStateException("CacheConfig does not exist!!! name: " + name);
            }
            cacheConfig.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
        } else if (cacheConfig != null) {
            //UNREGISTER
            cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(cacheEntryListenerConfiguration);
        out.writeBoolean(register);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        cacheEntryListenerConfiguration = in.readObject();
        register = in.readBoolean();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.LISTENER_REGISTRATION;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

}

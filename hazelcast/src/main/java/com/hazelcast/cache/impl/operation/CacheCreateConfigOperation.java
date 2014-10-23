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

import java.io.IOException;

/**
 * Used to create cluster wide cache configuration.
 * <p>This configuration is created using the following algorithm;
 * <ul>
 * <li>Find partition id using the distributed object name of cache as a key.</li>
 * <li>Send the <code>CacheCreateConfigOperation</code> operation to the calculated partition which will force all
 * clusters to be single threaded.</li>
 * <li>{@link CacheService#createCacheConfigIfAbsent(com.hazelcast.config.CacheConfig, boolean)} is called.</li>
 * </ul></p>
 * <p>This operation's purpose is to pass the required parameters into
 * {@link CacheService#createCacheConfigIfAbsent(com.hazelcast.config.CacheConfig, boolean)}.</p>
 */
public class CacheCreateConfigOperation
        extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private CacheConfig config;
    private boolean isLocal;

    private transient Object response;

    public CacheCreateConfigOperation() {
    }

    public CacheCreateConfigOperation(CacheConfig config) {
        this(config, false);
    }

    public CacheCreateConfigOperation(CacheConfig config, boolean isLocal) {
        super(config.getNameWithPrefix());
        this.config = config;
        this.isLocal = isLocal;
    }

    @Override
    public void run()
            throws Exception {
        final CacheService service = getService();
        response = service.createCacheConfigIfAbsent(config, isLocal);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(config);
        out.writeBoolean(isLocal);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        config = in.readObject();
        isLocal = in.readBoolean();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CREATE_CONFIG;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

}

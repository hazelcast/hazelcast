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

package com.hazelcast.cache.operation;

import com.hazelcast.cache.CacheDataSerializerHook;
import com.hazelcast.cache.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.Member;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.Collection;

/**
 * @author mdogan 05/02/14
 */
public class CacheCreateConfigOperation extends AbstractNamedOperation implements IdentifiedDataSerializable {

    private CacheConfig config;
    private boolean create;

    private transient Object response;

    public CacheCreateConfigOperation() {
    }

    public CacheCreateConfigOperation(String name, CacheConfig config, boolean create) {
        super(name);
        this.config = config;
        this.create = create;
    }


    @Override
    public void run() throws Exception {
        final CacheService service = getService();
        if(create){
            response = service.createCacheConfigIfAbsent(config);
        } else {
            response = service.updateCacheConfig(config);
        }
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
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(config);
        out.writeBoolean(create);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        config = in.readObject();
        create = in.readBoolean();
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

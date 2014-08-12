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

package com.hazelcast.cache.jsr107.client;

import com.hazelcast.cache.jsr107.operation.CacheClearOperationFactory;
import com.hazelcast.cache.jsr107.CachePortableHook;
import com.hazelcast.cache.jsr107.CacheService;
import com.hazelcast.client.AllPartitionsClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.security.Permission;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CacheClearRequest extends AllPartitionsClientRequest implements RetryableRequest {

    private String name;
    private Set<Data> keys= null ;
    private  boolean isRemoveAll=false;

    public CacheClearRequest() {
    }

    public CacheClearRequest(String name, Set<Data> keys, boolean isRemoveAll) {
        this.name = name;
        this.keys = keys;
        this.isRemoveAll = isRemoveAll;
    }

    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    public int getClassId() {
        return CachePortableHook.CLEAR;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeBoolean(isRemoveAll);
        out.writeBoolean(keys != null);
        if(keys != null){
            out.write(keys.size());
            for(Data key:keys){
                key.writeData(out);
            }
        }

    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        isRemoveAll = in.readBoolean();
        boolean isKeysNotNull = in.readBoolean();
        if(isKeysNotNull){
            int size = in.readInt();
            keys= new HashSet<Data>(size);
            for(int i=0; i< size; i++){
                Data key = new Data();
                key.readData(in);
                keys.add(key);
            }
        }
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new CacheClearOperationFactory(name,keys, isRemoveAll);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}

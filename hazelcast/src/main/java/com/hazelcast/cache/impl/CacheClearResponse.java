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

package com.hazelcast.cache.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.Serializable;

/**
 * This class is a simple object wrapper to be used by {@link com.hazelcast.cache.impl.operation.CacheClearOperation}.
 * The response returned or exception thrown is wrapped into a
 * single {@link com.hazelcast.cache.impl.CacheClearResponse}
 * so that multiple operations' result can be returned using a collection.
 *
 */
public class CacheClearResponse
        implements IdentifiedDataSerializable, Serializable {

    private Object response;

    public CacheClearResponse() {
    }

    public CacheClearResponse(Object response) {
        this.response = response;
    }

    public Object getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeObject(response);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        response = in.readObject();
    }

}

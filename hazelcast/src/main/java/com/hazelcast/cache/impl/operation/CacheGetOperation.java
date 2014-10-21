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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReadonlyOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Cache Get Operation,
 * <p>Provides the cache get functionality.</p>
 * @see com.hazelcast.cache.impl.ICacheRecordStore#get(Data, javax.cache.expiry.ExpiryPolicy)
 */
public class CacheGetOperation
        extends AbstractCacheOperation
        implements ReadonlyOperation {

    private ExpiryPolicy expiryPolicy;

    public CacheGetOperation() {
    }

    public CacheGetOperation(String name, Data key, ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public void run()
            throws Exception {
        response = cache.get(key, expiryPolicy);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET;
    }
}

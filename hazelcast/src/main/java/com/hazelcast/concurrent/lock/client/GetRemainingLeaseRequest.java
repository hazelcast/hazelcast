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

package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.concurrent.lock.*;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @mdogan 5/3/13
 */
public final class GetRemainingLeaseRequest extends KeyBasedClientRequest implements Portable {

    private Data key;

    public GetRemainingLeaseRequest() {
    }

    public GetRemainingLeaseRequest(Data key) {
        this.key = key;
    }

    protected final Operation prepareOperation() {
        return new GetRemainingLeaseTimeOperation(new InternalLockNamespace(), key);
    }

    protected final Object getKey() {
        return key;
    }

    public final String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return LockPortableHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return LockPortableHook.GET_REMAINING_LEASE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
    }

    public void readPortable(PortableReader reader) throws IOException {
        ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
    }

}

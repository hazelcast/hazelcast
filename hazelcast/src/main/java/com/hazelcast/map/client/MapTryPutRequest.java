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

import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.operation.TryPutOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MapTryPutRequest extends MapPutRequest {

    private long timeout;

    public MapTryPutRequest() {
    }

    public MapTryPutRequest(String name, Data key, Data value, long threadId, long timeout) {
        super(name, key, value, threadId, -1);
        this.timeout = timeout;
    }

    public int getClassId() {
        return MapPortableHook.TRY_PUT;
    }

    @Override
    protected Operation prepareOperation() {
        TryPutOperation op = new TryPutOperation(name, key, value, timeout);
        op.setThreadId(threadId);
        return op;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeLong("timeout", timeout);
        super.write(writer);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        timeout = reader.readLong("timeout");
        super.read(reader);
    }

    @Override
    public String getMethodName() {
        return "tryPut";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key, value, timeout, TimeUnit.MILLISECONDS};
    }
}

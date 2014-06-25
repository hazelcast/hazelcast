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

package com.hazelcast.queue.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.OfferOperation;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Provides the request service for {@link com.hazelcast.queue.OfferOperation}
 */
public class OfferRequest extends QueueRequest {

    private Data data;

    public OfferRequest() {
    }

    public OfferRequest(String name, Data data) {
        super(name);
        this.data = data;
    }

    public OfferRequest(String name, long timeoutMillis, Data data) {
        super(name, timeoutMillis);
        this.data = data;
    }

    @Override
    protected Operation prepareOperation() {
        return new OfferOperation(name, timeoutMillis, data);
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.OFFER;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        data.writeData(out);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        data = new Data();
        data.readData(in);
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_ADD);
    }

    @Override
    public String getMethodName() {
        if (timeoutMillis == -1) {
            return "put";
        }
        return "offer";
    }

    @Override
    public Object[] getParameters() {
        if (timeoutMillis > 0) {
            return new Object[]{data, timeoutMillis, TimeUnit.MILLISECONDS};
        }
        return new Object[]{data};
    }
}

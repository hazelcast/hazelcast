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

package com.hazelcast.queue.impl.client;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.impl.operations.DrainOperation;
import com.hazelcast.queue.impl.QueuePortableHook;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.SerializableCollection;

import java.io.IOException;
import java.security.Permission;
import java.util.Collection;

/**
 * Provides the request service for {@link com.hazelcast.queue.impl.operations.DrainOperation}
 */
public class DrainRequest extends QueueRequest {

    private int maxSize;

    public DrainRequest() {
    }

    public DrainRequest(String name, int maxSize) {
        super(name);
        this.maxSize = maxSize;
    }

    @Override
    protected Operation prepareOperation() {
        return new DrainOperation(name, maxSize);
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.DRAIN;
    }

    @Override
    protected Object filter(Object response) {
        if (response instanceof SerializableCollection) {
            Collection<Data> coll = ((SerializableCollection) response).getCollection();
            return new PortableCollection(coll);
        }
        return super.filter(response);
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeInt("m", maxSize);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        maxSize = reader.readInt("m");
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "drainTo";
    }

    @Override
    public Object[] getParameters() {
        if (maxSize == -1) {
            return new Object[]{null};
        }
        return new Object[]{null, maxSize};
    }
}

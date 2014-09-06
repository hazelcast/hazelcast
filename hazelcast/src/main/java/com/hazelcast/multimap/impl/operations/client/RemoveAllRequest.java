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

package com.hazelcast.multimap.impl.operations.client;

import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.multimap.impl.operations.RemoveAllOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PortableCollection;

import java.io.IOException;
import java.security.Permission;
import java.util.Collection;

import static com.hazelcast.multimap.impl.MultiMapContainerSupport.createCollection;

public class RemoveAllRequest extends MultiMapKeyBasedRequest {

    long threadId;

    public RemoveAllRequest() {
    }

    public RemoveAllRequest(String name, Data key, long threadId) {
        super(name, key);
        this.threadId = threadId;
    }

    protected Operation prepareOperation() {
        return new RemoveAllOperation(name, key, threadId);
    }

    public int getClassId() {
        return MultiMapPortableHook.REMOVE_ALL;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeLong("t", threadId);
        super.write(writer);
    }

    public void read(PortableReader reader) throws IOException {
        threadId = reader.readLong("t");
        super.read(reader);
    }

    protected Object filter(Object response) {
        if (response instanceof MultiMapResponse) {
            Collection<MultiMapRecord> responseCollection = ((MultiMapResponse) response).getCollection();
            if (responseCollection == null) {
                return new PortableCollection();
            }
            Collection<Data> collection = createCollection(responseCollection);
            for (MultiMapRecord record : responseCollection) {
                collection.add(serializationService.toData(record.getObject()));
            }
            return new PortableCollection(collection);
        }
        return super.filter(response);
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "remove";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key};
    }
}

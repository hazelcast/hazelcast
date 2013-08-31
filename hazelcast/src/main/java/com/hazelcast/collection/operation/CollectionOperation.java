/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.operation;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ListContainer;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.io.IOException;

/**
 * @ali 8/30/13
 */
public abstract class CollectionOperation extends Operation implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    protected transient Object response;

    private transient CollectionContainer container;

    protected CollectionOperation() {
    }

    protected CollectionOperation(String name) {
        this.name = name;
    }

    protected final ListContainer getOrCreateListContainer(){
        if (container == null) {
            ListService service = getService();
            try {
                container = service.getOrCreateContainer(name, this instanceof BackupOperation);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        return (ListContainer) container;
    }

    protected final CollectionContainer getOrCreateContainer(){
        if (container == null) {
            CollectionService service = getService();
            try {
                container = service.getOrCreateContainer(name, this instanceof BackupOperation);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        return container;
    }

    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    public boolean returnsResponse() {
        return true;
    }

    public Object getResponse() {
        return response;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }
}

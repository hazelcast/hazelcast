/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.map.impl.ImmutableMapSupport;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;

public class ContainsValueOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private boolean contains;
    private Object testValue;

    public ContainsValueOperation(String name, Object testValue) {
        super(name);
        this.testValue = testValue;
    }

    public ContainsValueOperation() {
    }

    @Override
    protected void runInternal() {

        if (mapContainer.getMapConfig().getInMemoryFormat() == InMemoryFormat.OBJECT) {

            if (testValue instanceof Data) {
                // At this point, testValue should be deserialized. So, it cannot be an instance of Data
                throw new IllegalStateException("Unexpected type for value in containsValue. " + testValue);
            } else {
                if (ImmutableMapSupport.isConsideredImmutable(testValue, this)) {
                    contains = recordStore.containsValue(testValue);
                } else {
                    contains = recordStore.containsValue(ImmutableMapSupport.defensiveCopy(testValue, mapServiceContext));
                }
            }
        } else {
            contains = recordStore.containsValue(testValue);
        }
    }

    @Override
    public Object getResponse() {
        return contains;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeObject(out, testValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        testValue = IOUtil.readObject(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.CONTAINS_VALUE;
    }
}

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

package com.hazelcast.map;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;

public class ContainsValueOperation extends AbstractNamedOperation implements PartitionAwareOperation {

    private boolean contains = false;
    private Data testValue;

    public ContainsValueOperation(String name, Data testValue) {
        super(name);
        this.testValue = testValue;
    }

    public ContainsValueOperation() {
    }

    public void run() {
        MapService mapService = (MapService) getService();
        RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        contains = recordStore.containsValue(testValue);
    }

    @Override
    public Object getResponse() {
        return contains;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, testValue);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        testValue = IOUtil.readNullableData(in);
    }
}

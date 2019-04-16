/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl.operations;

import com.hazelcast.datastream.impl.DSDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.function.Supplier;

import java.io.IOException;

public class FillOperation extends DataStreamOperation {

    private long count;
    private Supplier supplier;

    public FillOperation() {
    }

    public FillOperation(String name, Supplier supplier, long count) {
        super(name);
        this.supplier = supplier;
        this.count = count;
    }

    @Override
    public void run() {
        getLogger().info("Executing fill operation:" + getPartitionId() + " count:" + count);

        for (long k = 0; k < count; k++) {
            partition.append(supplier.get());
        }
    }

    @Override
    public int getId() {
        return DSDataSerializerHook.FILL_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(count);
        out.writeObject(supplier);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        count = in.readLong();
        supplier = in.readObject();
    }
}

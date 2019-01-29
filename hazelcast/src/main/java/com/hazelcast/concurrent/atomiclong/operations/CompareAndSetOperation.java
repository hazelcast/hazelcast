/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomiclong.operations;

import com.hazelcast.concurrent.atomiclong.AtomicLongContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

import static com.hazelcast.concurrent.atomiclong.AtomicLongDataSerializerHook.COMPARE_AND_SET;

public class CompareAndSetOperation extends AtomicLongBackupAwareOperation implements MutatingOperation {

    private long expect;
    private long update;
    private boolean returnValue;

    public CompareAndSetOperation() {
    }

    public CompareAndSetOperation(String name, long expect, long update) {
        super(name);
        this.expect = expect;
        this.update = update;
    }

    @Override
    public void run() throws Exception {
        AtomicLongContainer container = getLongContainer();
        returnValue = container.compareAndSet(expect, update);
        shouldBackup = returnValue;
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public Operation getBackupOperation() {
        return new SetBackupOperation(name, update);
    }

    @Override
    public int getId() {
        return COMPARE_AND_SET;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(expect);
        out.writeLong(update);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expect = in.readLong();
        update = in.readLong();
    }
}

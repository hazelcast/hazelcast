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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

// author: sancar - 24.12.2012
public abstract class AtomicLongBaseOperation extends Operation implements PartitionAwareOperation {

    protected String name;

    public AtomicLongBaseOperation() {
        super();
    }

    public AtomicLongBaseOperation(String name) {
        this.name = name;
    }

    public AtomicLongWrapper getNumber() {
        return ((AtomicLongService) getService()).getNumber(name);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

}

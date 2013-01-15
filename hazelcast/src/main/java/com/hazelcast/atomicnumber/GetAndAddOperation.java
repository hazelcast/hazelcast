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

package com.hazelcast.atomicnumber;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;

// author: sancar - 24.12.2012
public class GetAndAddOperation extends AtomicNumberBackupAwareOperation {

    private long delta;
    private long returnValue;


    public GetAndAddOperation() {
        super();
    }

    public GetAndAddOperation(String name, long delta) {
        super(name);
        this.delta = delta;
    }

    @Override
    public void run() throws Exception {
        returnValue = getNumber();
        setNumber(returnValue + delta);

    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(delta);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        delta = in.readLong();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name, returnValue + delta);
    }
}

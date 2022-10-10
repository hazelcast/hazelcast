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

package com.hazelcast.internal.alto;

import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;

import java.io.EOFException;

public abstract class Op {

    public final static int COMPLETED = 0;
    public final static int BLOCKED = 1;
    public final static int EXCEPTION = 2;

    public int partitionId;
    public long callId;
    public Managers managers;
    public int opcode;
    public StringBuffer name = new StringBuffer();
    public IOBuffer request;
    public IOBuffer response;
    public OpAllocator allocator;
    public OpScheduler scheduler;
    public Eventloop eventloop;

    public Op(int opcode) {
        this.opcode = opcode;
    }

    public void readName() throws EOFException {
        name.setLength(0);
        request.readString(name);

        //System.out.println("Read name: "+name);
    }

    public abstract int run() throws Exception;

    public void clear() {
    }

    public void release() {
        if (allocator != null) {
            allocator.free(this);
        }
    }
}

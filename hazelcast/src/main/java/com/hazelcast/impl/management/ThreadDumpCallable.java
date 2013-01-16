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

package com.hazelcast.impl.management;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

public class ThreadDumpCallable implements Callable<String>, DataSerializable {

    private static final long serialVersionUID = -1910495089344606344L;

    private boolean isDeadlock;

    public ThreadDumpCallable() {
        this(false);
    }

    public ThreadDumpCallable(boolean deadlock) {
        super();
        this.isDeadlock = deadlock;
    }

    public String call() throws Exception {
        ThreadDumpGenerator gen = ThreadDumpGenerator.newInstance();
        String result = isDeadlock ? gen.dumpDeadlocks() : gen.dumpAllThreads();
        return result;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeBoolean(isDeadlock);
    }

    public void readData(DataInput in) throws IOException {
        isDeadlock = in.readBoolean();
    }
}

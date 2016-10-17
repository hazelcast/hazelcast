/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.operation;

import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.internal.management.ThreadDumpGenerator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 *  Operation for generating thread dumps.
 */
public class ThreadDumpOperation extends AbstractManagementOperation {

    private boolean dumpDeadlocks;
    private String result;

    @SuppressWarnings("unused")
    public ThreadDumpOperation() {
        this(false);
    }

    public ThreadDumpOperation(boolean dumpDeadlocks) {
        this.dumpDeadlocks = dumpDeadlocks;
    }

    @Override
    public void run() throws Exception {
        result = dumpDeadlocks ? ThreadDumpGenerator.dumpDeadlocks() : ThreadDumpGenerator.dumpAllThreads();
    }

    public Object getResponse() {
        return result;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeBoolean(dumpDeadlocks);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        dumpDeadlocks = in.readBoolean();
    }

    @Override
    public int getId() {
        return ManagementDataSerializerHook.THREAD_DUMP;
    }
}

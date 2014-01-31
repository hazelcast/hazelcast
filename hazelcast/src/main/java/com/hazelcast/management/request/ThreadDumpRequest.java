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

package com.hazelcast.management.request;

import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.management.operation.ThreadDumpOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.nio.IOUtil.readLongString;
import static com.hazelcast.nio.IOUtil.writeLongString;

public class ThreadDumpRequest implements ConsoleRequest {

    private boolean dumpDeadlocks;
    private Address target;

    public ThreadDumpRequest() {
    }

    public ThreadDumpRequest(Address target, boolean dumpDeadlocks) {
        this.target = target;
        this.dumpDeadlocks = dumpDeadlocks;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_THREAD_DUMP;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
        String threadDump = (String) mcs.callOnAddress(target, new ThreadDumpOperation(dumpDeadlocks));
        if (threadDump != null) {
            dos.writeBoolean(true);
            writeLongString(dos, threadDump);
        } else {
            dos.writeBoolean(false);
        }
    }

    @Override
    public String readResponse(ObjectDataInput in) throws IOException {
        if (in.readBoolean()) {
            return readLongString(in);
        } else {
            return null;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        target.writeData(out);
        out.writeBoolean(dumpDeadlocks);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        target = new Address();
        target.readData(in);
        dumpDeadlocks = in.readBoolean();
    }
}

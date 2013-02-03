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

import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.readLongString;
import static com.hazelcast.nio.IOUtil.writeLongString;

public class ThreadDumpRequest implements ConsoleRequest {

    private boolean isDeadlock;
    private Address target;

    public ThreadDumpRequest() {
    }

    public ThreadDumpRequest(Address target, boolean deadlock) {
        this.target = target;
        this.isDeadlock = deadlock;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_THREAD_DUMP;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        String threadDump = (String) mcs.call(target, new ThreadDumpCallable(isDeadlock));
        if (threadDump != null) {
            dos.writeBoolean(true);
            writeLongString(dos, threadDump);
        } else {
            dos.writeBoolean(false);
        }
    }

    public String readResponse(DataInput in) throws IOException {
        if (in.readBoolean()) {
            return readLongString(in);
        } else {
            return null;
        }
    }

    public void writeData(DataOutput out) throws IOException {
        target.writeData(out);
        out.writeBoolean(isDeadlock);
    }

    public void readData(DataInput in) throws IOException {
        target = new Address();
        target.readData(in);
        isDeadlock = in.readBoolean();
    }
}

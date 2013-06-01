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

import com.hazelcast.management.ConsoleCommandHandler;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.nio.IOUtil.readLongString;
import static com.hazelcast.nio.IOUtil.writeLongString;

public class ConsoleCommandRequest implements ConsoleRequest {

    private String command;

    public ConsoleCommandRequest() {
    }

    public ConsoleCommandRequest(String command) {
        super();
        this.command = command;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CONSOLE_COMMAND;
    }

    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
        ConsoleCommandHandler handler = mcs.getCommandHandler();
        try {
            final String output = handler.handleCommand(command);
            writeLongString(dos, output);
        } catch (Throwable e) {
            writeLongString(dos, "Error: " + e.getClass().getSimpleName() + "[" + e.getMessage() + "]");
        }
    }

    public Object readResponse(ObjectDataInput in) throws IOException {
        return readLongString(in);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(command);
    }

    public void readData(ObjectDataInput in) throws IOException {
        command = in.readUTF();
    }
}

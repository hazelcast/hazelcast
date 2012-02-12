/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LoginRequest implements ConsoleRequest {
    String groupName;
    String password;

    public LoginRequest() {
    }

    public LoginRequest(String groupName, String password) {
        this.groupName = groupName;
        this.password = password;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOGIN;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        boolean success = mcs.login(groupName, password);
        dos.writeBoolean(success);
        if (!success) {
            Thread.sleep(3000);
            throw new IOException("Invalid login!");
        }
    }

    public Boolean readResponse(DataInput in) throws IOException {
        return in.readBoolean();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeUTF(password);
    }

    public void readData(DataInput in) throws IOException {
        groupName = in.readUTF();
        password = in.readUTF();
    }
}
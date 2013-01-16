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

package com.hazelcast.cluster;

import com.hazelcast.config.GroupConfig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AuthorizationCall extends AbstractRemotelyCallable<Boolean> {

    String groupName;
    String groupPassword;

    public AuthorizationCall() {
    }

    public AuthorizationCall(String groupName, String groupPassword) {
        this.groupName = groupName;
        this.groupPassword = groupPassword;
    }

    public Boolean call() throws Exception {
        GroupConfig groupConfig = node.getConfig().getGroupConfig();
        if (!groupName.equals(groupConfig.getName())) return Boolean.FALSE;
        if (!groupPassword.equals(groupConfig.getPassword())) return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        groupName = in.readUTF();
        groupPassword = in.readUTF();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
    }
}

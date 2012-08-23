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

package com.hazelcast.cluster;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.impl.spi.AbstractOperation;
import com.hazelcast.impl.spi.NonBlockingOperation;
import com.hazelcast.impl.spi.NonMemberOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AuthorizationCall extends AbstractOperation implements NonBlockingOperation, NonMemberOperation {

    String groupName;
    String groupPassword;

    public AuthorizationCall() {
    }

    public AuthorizationCall(String groupName, String groupPassword) {
        this.groupName = groupName;
        this.groupPassword = groupPassword;
    }

    public void run() {
        GroupConfig groupConfig = getNodeService().getNode().getConfig().getGroupConfig();
        Boolean response = Boolean.TRUE;
        if (!groupName.equals(groupConfig.getName())) {
            response = Boolean.FALSE;
        } else if (!groupPassword.equals(groupConfig.getPassword())) {
            response = Boolean.FALSE;
        }
        getResponseHandler().sendResponse(response);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        groupName = in.readUTF();
        groupPassword = in.readUTF();
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
    }
}

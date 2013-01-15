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

import com.hazelcast.impl.base.SystemLogRecord;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

// author: sancar - 12.12.2012
public class GetLogsRequest implements ConsoleRequest {

    public GetLogsRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOGS;
    }

    public Object readResponse(DataInput in) throws IOException {
        List<SystemLogRecord> list = new LinkedList<SystemLogRecord>();
        String node = in.readUTF();
        int size = in.readInt();
        for(int i = 0; i < size ; i++){
            SystemLogRecord systemLogRecord = new SystemLogRecord();
            systemLogRecord.readData(in);
            systemLogRecord.setNode(node);
            list.add(systemLogRecord);
        }
        return list;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        List<SystemLogRecord> logBundle = mcs.getHazelcastInstance().node.getSystemLogService().getLogBundle();
        final Address address = mcs.getHazelcastInstance().node.getThisAddress();
        dos.writeUTF(address.getHost() + ":" + address.getPort() );
        dos.writeInt(logBundle.size());
        for (SystemLogRecord systemLogRecord : logBundle) {
            systemLogRecord.writeData(dos);
        }
    }

    public void writeData(DataOutput out) throws IOException {

    }

    public void readData(DataInput in) throws IOException {

    }
}

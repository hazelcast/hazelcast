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

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.SystemLogRecord;
import com.hazelcast.logging.SystemLogService;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

public class GetLogsRequest implements ConsoleRequest {

    public GetLogsRequest() {
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOGS;
    }

    @Override
    public Object readResponse(JsonObject in) {
//        List<SystemLogRecord> list = new LinkedList<SystemLogRecord>();
//        String node = in.readUTF();
//        int size = in.readInt();
//        for (int i = 0; i < size; i++) {
//            SystemLogRecord systemLogRecord = new SystemLogRecord();
//            systemLogRecord.readData(in);
//            systemLogRecord.setNode(node);
//            list.add(systemLogRecord);
//        }
//        return list;
        return null;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject os) throws Exception {
//        Node node = mcs.getHazelcastInstance().node;
//        SystemLogService systemLogService = node.getSystemLogService();
//        List<SystemLogRecord> logBundle = systemLogService.getLogBundle();
//        Address address = node.getThisAddress();
//        dos.writeUTF(address.getHost() + ":" + address.getPort());
//        dos.writeInt(logBundle.size());
//        for (SystemLogRecord systemLogRecord : logBundle) {
//            systemLogRecord.writeData(dos);
//        }
    }

    @Override
    public JsonValue toJson() {
        return null;
    }

    @Override
    public void fromJson(JsonObject json) {

    }
}

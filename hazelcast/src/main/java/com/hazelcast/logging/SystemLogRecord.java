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

package com.hazelcast.logging;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class SystemLogRecord implements Comparable, DataSerializable {

    private long date;
    private String node;
    private String message;
    private String type;
    private long callId;

    public SystemLogRecord(long callId, String node, long date, String message, String type) {
        this.callId = callId;
        this.node = node;
        this.date = date;
        this.message = message;
        this.type = type;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public SystemLogRecord() {
    }

    public long getCallId() {
        return callId;
    }

    public void setCallId(long callId) {
        this.callId = callId;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int compareTo(Object o) {
        long thisVal = this.date;
        SystemLogRecord other = (SystemLogRecord) o;
        long anotherVal = other.getDate();
        return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(callId);
        out.writeLong(date);
        out.writeUTF(message);
        out.writeUTF(type);
    }

    public void readData(ObjectDataInput in) throws IOException {
        callId = in.readLong();
        date = in.readLong();
        message = in.readUTF();
        type = in.readUTF();
    }
}

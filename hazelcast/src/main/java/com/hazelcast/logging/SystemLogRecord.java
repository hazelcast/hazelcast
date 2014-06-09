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

import com.eclipsesource.json.JsonObject;
import com.hazelcast.management.JsonSerializable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;

import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getString;

public class SystemLogRecord implements Comparable<SystemLogRecord>, DataSerializable, JsonSerializable {

    private long date;
    private String node;
    private String message;
    private String type;

    public SystemLogRecord() {
    }

    public SystemLogRecord(long date, String message, String type) {
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

    @Override
    public int compareTo(SystemLogRecord o) {
        long thisVal = this.date;
        long anotherVal = o.getDate();
        if (thisVal < anotherVal) {
            return -1;
        } else {
            return thisVal == anotherVal ? 0 : 1;
        }
    }

    @Override
    public int hashCode() {
        return (int) (date ^ (date >>> 32));
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof SystemLogRecord) {
            return this.compareTo((SystemLogRecord) o) == 0;
        }
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(date);
        out.writeUTF(message);
        out.writeUTF(type);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        date = in.readLong();
        message = in.readUTF();
        type = in.readUTF();
    }

    @Override
    public String toString() {
        return "SystemLogRecord{"
                + "date=" + date
                + ", node='" + node + '\''
                + ", message='" + message + '\''
                + ", type='" + type + '\''
                + '}';
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("date", date);
        root.add("message", message);
        root.add("type", type);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        date = getLong(json, "date");
        message = getString(json, "message");
        type = getString(json, "type");
    }
}

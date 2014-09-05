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

package com.hazelcast.client.impl.client;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public final class GenericError implements Portable {

    private String name;
    private String message;
    private String details;
    private int type;

    public GenericError() {
    }

    public GenericError(String name, String message, int type) {
        this(name, message, null, type);
    }

    public GenericError(String name, String message, String details, int type) {
        this.name = name;
        this.message = message;
        this.details = details;
        this.type = type;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.GENERIC_ERROR;
    }

    public String getName() {
        return name;
    }

    public String getMessage() {
        return message;
    }

    public String getDetails() {
        return details;
    }

    public int getType() {
        return type;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("m", message);
        writer.writeUTF("d", details);
        writer.writeInt("t", type);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        message = reader.readUTF("m");
        details = reader.readUTF("d");
        type = reader.readInt("t");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenericError{");
        sb.append("message='").append(message).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}

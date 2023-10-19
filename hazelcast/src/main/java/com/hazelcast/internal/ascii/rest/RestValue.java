/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.TextProtocolsDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class RestValue implements IdentifiedDataSerializable {

    private byte[] value;
    private byte[] contentType;

    public RestValue() {
    }

    public RestValue(byte[] value, byte[] contentType) {
        this.value = value;
        this.contentType = contentType;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readByteArray();
        contentType = in.readByteArray();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(value);
        out.writeByteArray(contentType);
    }

    public byte[] getContentType() {
        return contentType;
    }

    public void setContentType(byte[] contentType) {
        this.contentType = contentType;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        String contentTypeStr;
        if (contentType == null) {
            contentTypeStr = "unknown-content-type";
        } else {
            contentTypeStr = new String(contentType, StandardCharsets.UTF_8);
        }

        String valueStr;
        if (value == null) {
            valueStr = "value.length=0";
        } else if (contentTypeStr.contains("text")) {
            valueStr = "value=\"" + new String(value, StandardCharsets.UTF_8) + "\"";
        } else {
            valueStr = "value.length=" + value.length;
        }
        return "RestValue{"
                + "contentType='"
                + contentTypeStr
                + "', " + valueStr
                + '}';
    }

    @Override
    public int getFactoryId() {
        return TextProtocolsDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TextProtocolsDataSerializerHook.REST_VALUE;
    }
}

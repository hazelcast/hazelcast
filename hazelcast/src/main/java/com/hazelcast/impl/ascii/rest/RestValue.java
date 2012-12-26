/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.ascii.rest;

import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RestValue implements DataSerializable {
    private byte[] value;
    private byte[] contentType;

    public RestValue() {
    }

    public RestValue(byte[] value, byte[] contentType) {
        this.value = value;
        this.contentType = contentType;
    }

    public void readData(DataInput in) throws IOException {
        value = SerializationHelper.readByteArray(in);
        contentType = SerializationHelper.readByteArray(in);
    }

    public void writeData(DataOutput out) throws IOException {
        SerializationHelper.writeByteArray(out, value);
        SerializationHelper.writeByteArray(out, contentType);
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
        String contentTypeStr = (contentType == null) ? "unknown-content-type" : new String(contentType);
        String valueStr;
        if (value == null) {
            valueStr = "value.length=0";
        } else if (contentTypeStr.contains("text")) {
            valueStr = "value=\"" + new String(value) + "\"";
        } else {
            valueStr = "value.length=" + value.length;
        }
        return "RestValue{" +
                "contentType='" + contentTypeStr +
                "', " + valueStr +
                '}';
    }
}
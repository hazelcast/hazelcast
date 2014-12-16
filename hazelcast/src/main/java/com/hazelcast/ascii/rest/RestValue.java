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

package com.hazelcast.ascii.rest;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

import static com.hazelcast.util.StringUtil.bytesToString;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("EI_EXPOSE_REP")
public class RestValue implements DataSerializable {
    private byte[] value;
    private byte[] contentType;

    public RestValue() {
    }

    public RestValue(byte[] value, byte[] contentType) {
        this.value = value;
        this.contentType = contentType;
    }

    public void readData(ObjectDataInput in) throws IOException {
        value = in.readByteArray();
        contentType = in.readByteArray();
    }

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
            contentTypeStr = bytesToString(contentType);
        }

        String valueStr;
        if (value == null) {
            valueStr = "value.length=0";
        } else if (contentTypeStr.contains("text")) {
            valueStr = "value=\"" + bytesToString(value) + "\"";
        } else {
            valueStr = "value.length=" + value.length;
        }
        return "RestValue{"
                + "contentType='"
                + contentTypeStr
                + "', " + valueStr
                + '}';
    }
}

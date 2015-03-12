/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.SerializationService;

public class ClientMessageWriterImpl implements ClientMessageWriter {

    public ClientMessageWriterImpl(SerializationService serializationService) {
    }

    public ClientMessageWriterImpl setCallId(int callId) {
        return this;
    }

    public ClientMessageWriterImpl setMessageId(long messageId) {
        return this;
    }

    public ClientMessageWriterImpl setPartitionId(int partitionId) {
        return this;
    }

    public ClientMessageWriterImpl setVersion(long version) {
        return this;
    }


    public ClientMessage getClientMessage() {
        return new ClientMessage();
    }


    public void appendInt(int value) {

    }


    public void appendLong(long value) {
    }


    public void appendUTF(String value) {
    }


    public void appendBoolean(boolean value) {
    }


    public void appendByte(byte value) {

    }


    public void appendChar(char value) {

    }


    public void appendDouble(double value) {

    }


    public void appendFloat(float value) {

    }


    public void appendShort(short value) {

    }


    public void appendObject(Object portable) {
    }


    public void appendNullPortable(int factoryId, int classId) {

    }


    public void appendByteArray(byte[] bytes) {

    }


    public void appendCharArray(char[] chars) {

    }


    public void appendIntArray(int[] ints) {

    }


    public void appendLongArray(long[] longs) {

    }


    public void appendDoubleArray(double[] values) {

    }


    public void appendFloatArray(float[] values) {

    }


    public void appendShortArray(short[] values) {

    }


    public void appendPortableArray(Portable[] portables) {

    }

    public void appendData(Data data) {
    }
}

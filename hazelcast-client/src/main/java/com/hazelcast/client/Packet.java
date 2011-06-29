/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.util.ByteUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class Packet {

    private byte[] key;

    private byte[] value;

    private String name;

    private ClusterOperation operation;

    private int threadId;

    private long ttl = -1;

    private long timeout = -1;

    private long longValue;

    private byte responseType = Constants.ResponseTypes.RESPONSE_NONE;

    private long callId = -1;

    private static final byte PACKET_VERSION = GroupProperties.PACKET_VERSION.getByte();

    public Packet() {
    }

    public void reset() {
        name = null;
        operation = null;
        key = null;
        value = null;
        threadId = -1;
        ttl = -1;
        timeout = -1;
        longValue = 0;
        responseType = Constants.ResponseTypes.RESPONSE_NONE;
        callId = -1;
    }

    public void writeTo(PacketWriter packetWriter, DataOutputStream outputStream) throws IOException {
        final ByteBuffer writeHeaderBuffer = packetWriter.writeHeaderBuffer;
        writeHeaderBuffer.clear();
        writeHeaderBuffer.position(13);
        writeHeader(packetWriter);
        int size = writeHeaderBuffer.position();
        int headerSize = size - 13;
        writeHeaderBuffer.position(0);
        writeHeaderBuffer.putInt(headerSize);
        writeHeaderBuffer.putInt((key == null) ? 0 : key.length);
        writeHeaderBuffer.putInt((value == null) ? 0 : value.length);
        writeHeaderBuffer.put(PACKET_VERSION);
        outputStream.write(writeHeaderBuffer.array(), 0, size);
        if (key != null)
            outputStream.write(key);
        if (value != null)
            outputStream.write(value);
    }

    public void readFrom(PacketReader packetReader, DataInputStream dis) throws IOException {
        final ByteBuffer readHeaderBuffer = packetReader.readHeaderBuffer;
        final int headerSize = dis.readInt();
        int keySize = dis.readInt();
        int valueSize = dis.readInt();
        byte packetVersion = dis.readByte();
        if (packetVersion != PACKET_VERSION) {
            throw new ClusterClientException("Invalid packet version. Expected:"
                    + PACKET_VERSION + ", Found:" + packetVersion);
        }
        readHeaderBuffer.clear();
        readHeaderBuffer.limit(headerSize);
        dis.readFully(readHeaderBuffer.array(), 0, headerSize);
        this.operation = ClusterOperation.create(readHeaderBuffer.get());
        int blockId = readHeaderBuffer.getInt();
        this.threadId = readHeaderBuffer.getInt();
        byte booleans = readHeaderBuffer.get();
        if (ByteUtil.isTrue(booleans, 1)) {
            timeout = readHeaderBuffer.getLong();
        }
        if (ByteUtil.isTrue(booleans, 2)) {
            ttl = readHeaderBuffer.getLong();
        }
        if (ByteUtil.isTrue(booleans, 4)) {
            longValue = readHeaderBuffer.getLong();
        }
        if (!ByteUtil.isTrue(booleans, 7)) {
            throw new ClusterClientException("LockAddress cannot be sent to the client!" + operation);
        }
        this.callId = readHeaderBuffer.getLong();
        this.responseType = readHeaderBuffer.get();
        int nameLength = readHeaderBuffer.getInt();
        if (nameLength > 0) {
            byte[] b = new byte[nameLength];
            readHeaderBuffer.get(b);
            this.name = new String(b);
        }
        int indexCount = readHeaderBuffer.get();
        key = new byte[keySize];
        dis.readFully(key);
        value = new byte[valueSize];
        dis.readFully(value);
    }

    private void writeHeader(PacketWriter packetWriter) throws IOException {
        final ByteBuffer writeHeaderBuffer = packetWriter.writeHeaderBuffer;
        final Map<String, byte[]> nameCache = packetWriter.nameCache;
        writeHeaderBuffer.put(operation.getValue());
        writeHeaderBuffer.putInt(-1); //blockId
        writeHeaderBuffer.putInt(threadId);
        byte booleans = 0;
        // first bit is lockCount
        if (timeout != -1) {
            booleans = ByteUtil.setTrue(booleans, 1);
        }
        if (ttl != -1) {
            booleans = ByteUtil.setTrue(booleans, 2);
        }
        // txn always -1
        if (longValue != Long.MIN_VALUE) {
            booleans = ByteUtil.setTrue(booleans, 4);
        }
        //version always -1
        booleans = ByteUtil.setTrue(booleans, 6); // client = true
        booleans = ByteUtil.setTrue(booleans, 7); // lockAddressNull = true
        writeHeaderBuffer.put(booleans);
        if (timeout != -1) {
            writeHeaderBuffer.putLong(timeout);
        }
        if (ttl != -1) {
            writeHeaderBuffer.putLong(ttl);
        }
        if (longValue != Long.MIN_VALUE) {
            writeHeaderBuffer.putLong(longValue);
        }
        writeHeaderBuffer.putLong(callId);
        writeHeaderBuffer.put(responseType);
        int nameLen = 0;
        byte[] nameInBytes = null;
        if (name != null) {
            nameInBytes = nameCache.get(name);
            if (nameInBytes == null) {
                nameInBytes = name.getBytes();
                if (nameCache.size() > 10000) {
                    nameCache.clear();
                }
                nameCache.put(name, nameInBytes);
            }
            nameLen = nameInBytes.length;
        }
        writeHeaderBuffer.putInt(nameLen);
        if (nameLen > 0) {
            writeHeaderBuffer.put(nameInBytes);
        }
        writeHeaderBuffer.put((byte) 0);
    }

    public void set(String name, ClusterOperation operation,
                    byte[] key, byte[] value) {
        this.name = name;
        this.operation = operation;
        this.setKey(key);
        this.setValue(value);
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setCallId(long callId) {
        this.callId = callId;
    }

    public long getCallId() {
        return callId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ClusterOperation getOperation() {
        return operation;
    }

    public void setOperation(ClusterOperation operation) {
        this.operation = operation;
    }

    public int getThreadId() {
        return threadId;
    }

    public void setThreadId(int threadId) {
        this.threadId = threadId;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getLongValue() {
        return longValue;
    }

    public void setLongValue(long longValue) {
        this.longValue = longValue;
    }

    public byte getResponseType() {
        return responseType;
    }

    public void setResponseType(byte responseType) {
        this.responseType = responseType;
    }

    @Override
    public String toString() {
        return "Packet [callId = " + callId + "  name = " + name + " operation = " + operation + "]";
    }
}

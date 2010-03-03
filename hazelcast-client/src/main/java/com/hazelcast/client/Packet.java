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
import com.hazelcast.util.ByteUtil;

import java.io.*;

public class Packet {

    private int headerSize;

    private int keySize;

    private int valueSize;

    private byte[] headerInBytes;

    private byte[] key;

    private byte[] value;

    private String name;

    private ClusterOperation operation;

    private int blockId = 0;

    private int threadId;

    private int lockCount = 0;

    private long ttl = -1;

    private long timeout = -1;

    private long txnId = -1;

    private long longValue;

    private long version = -1;

    private byte responseType = Constants.ResponseTypes.RESPONSE_NONE;

    private long callId = -1;

    private byte indexCount = 0;

    private long[] indexes = new long[10];

    private byte[] indexTypes = new byte[10];

    private static final byte PACKET_VERSION = 4;

    public Packet() {
    }

    public void writeTo(DataOutputStream outputStream) throws IOException {
        headerInBytes = getHeader();
        headerSize = headerInBytes.length;
        outputStream.writeInt(headerSize);
        outputStream.writeInt(keySize);
        outputStream.writeInt(valueSize);
        outputStream.writeByte(PACKET_VERSION);
        outputStream.write(headerInBytes);
        if (key != null)
            outputStream.write(key);
        if (value != null)
            outputStream.write(value);
    }

    public void readFrom(DataInputStream dis) throws IOException {
        headerSize = dis.readInt();
        keySize = dis.readInt();
        valueSize = dis.readInt();
        byte packetVersion = dis.readByte();
        if (packetVersion != PACKET_VERSION) {
            throw new RuntimeException("Invalid packet version. Expected:"
                    + PACKET_VERSION + ", Found:" + packetVersion);
        }
        headerInBytes = new byte[headerSize];
        dis.readFully(headerInBytes);
        ByteArrayInputStream bis = new ByteArrayInputStream(headerInBytes);
        DataInputStream dis2 = new DataInputStream(bis);
        this.operation = ClusterOperation.create(dis2.readByte());
        this.blockId = dis2.readInt();
        this.threadId = dis2.readInt();
        byte booleans = dis2.readByte();
        if (ByteUtil.isTrue(booleans, 0)) {
            lockCount = dis2.readInt();
        }
        if (ByteUtil.isTrue(booleans, 1)) {
            timeout = dis2.readLong();
        }
        if (ByteUtil.isTrue(booleans, 2)) {
            ttl = dis2.readLong();
        }
        if (ByteUtil.isTrue(booleans, 3)) {
            txnId = dis2.readLong();
        }
        if (ByteUtil.isTrue(booleans, 4)) {
            longValue = dis2.readLong();
        }
        if (ByteUtil.isTrue(booleans, 5)) {
            version = dis2.readLong();
        }
        if (!ByteUtil.isTrue(booleans, 7)) {
            throw new RuntimeException("LockAddress cannot be sent to the client!" + operation);
        }
        this.callId = dis2.readLong();
        this.responseType = dis2.readByte();
        int nameLength = dis2.readInt();
        if (nameLength > 0) {
            byte[] b = new byte[nameLength];
            dis2.readFully(b);
            this.name = new String(b);
        }
        indexCount = dis2.readByte();
        for (int i = 0; i < indexCount; i++) {
            indexes[i] = dis2.readLong();
            indexTypes[i] = dis2.readByte();
        }
        key = new byte[keySize];
        dis.readFully(key);
        value = new byte[valueSize];
        dis.readFully(value);
    }

    private byte[] getHeader() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeByte(operation.getValue());
        dos.writeInt(blockId);
        dos.writeInt(threadId);
        byte booleans = 0;
        if (lockCount != 0) {
            booleans = ByteUtil.setTrue(booleans, 0);
        }
        if (timeout != -1) {
            booleans = ByteUtil.setTrue(booleans, 1);
        }
        if (ttl != -1) {
            booleans = ByteUtil.setTrue(booleans, 2);
        }
        if (txnId != -1) {
            booleans = ByteUtil.setTrue(booleans, 3);
        }
        if (longValue != Long.MIN_VALUE) {
            booleans = ByteUtil.setTrue(booleans, 4);
        }
        if (version != -1) {
            booleans = ByteUtil.setTrue(booleans, 5);
        }
        booleans = ByteUtil.setTrue(booleans, 6); // client = true
        booleans = ByteUtil.setTrue(booleans, 7); // lockAddressNull = true
        dos.writeByte(booleans);
        if (lockCount != 0) {
            dos.writeInt(lockCount);
        }
        if (timeout != -1) {
            dos.writeLong(timeout);
        }
        if (ttl != -1) {
            dos.writeLong(ttl);
        }
        if (txnId != -1) {
            dos.writeLong(txnId);
        }
        if (longValue != Long.MIN_VALUE) {
            dos.writeLong(longValue);
        }
        if (version != -1) {
            dos.writeLong(version);
        }
        dos.writeLong(callId);
        dos.writeByte(responseType);
        int nameLen = 0;
        byte[] nameInBytes = null;
        if (name != null) {
            nameInBytes = name.getBytes();
            nameLen = nameInBytes.length;
        }
        dos.writeInt(nameLen);
        if (nameLen > 0) {
            dos.write(nameInBytes);
        }
        dos.writeByte(indexCount);
        for (int i = 0; i < indexCount; i++) {
            dos.writeLong(indexes[i]);
            dos.writeByte(indexTypes[i]);
        }
        return bos.toByteArray();
    }

    public void set(String name, ClusterOperation operation,
                    byte[] key, byte[] value) {
        this.name = name;
        this.operation = operation;
        this.setKey(key);
        this.setValue(value);
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public int getValueSize() {
        return valueSize;
    }

    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
        if (key != null)
            keySize = this.key.length;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
        if (value != null)
            valueSize = this.value.length;
    }

    public void setCallId(long callid) {
        this.callId = callid;
    }

    public long getCallId() {
        return callId;
    }

    public byte[] getHeaderInBytes() {
        return headerInBytes;
    }

    public void setHeaderInBytes(byte[] headerInBytes) {
        this.headerInBytes = headerInBytes;
        headerSize = headerInBytes.length;
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

    public int getBlockId() {
        return blockId;
    }

    public void setBlockId(int blockId) {
        this.blockId = blockId;
    }

    public int getThreadId() {
        return threadId;
    }

    public void setThreadId(int threadId) {
        this.threadId = threadId;
    }

    public int getLockCount() {
        return lockCount;
    }

    public void setLockCount(int lockCount) {
        this.lockCount = lockCount;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public long getLongValue() {
        return longValue;
    }

    public void setLongValue(long longValue) {
        this.longValue = longValue;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public byte getResponseType() {
        return responseType;
    }

    public void setResponseType(byte responseType) {
        this.responseType = responseType;
    }

    public byte getIndexCount() {
        return indexCount;
    }

    public void setIndexCount(byte indexCount) {
        this.indexCount = indexCount;
    }

    public long[] getIndexes() {
        return indexes;
    }

    public void setIndexes(long[] indexes) {
        this.indexes = indexes;
    }

    public byte[] getIndexTypes() {
        return indexTypes;
    }

    public void setIndexTypes(byte[] indexTypes) {
        this.indexTypes = indexTypes;
    }

    public void clearForResponse() {
        this.name = null;
        this.key = null;
        this.value = null;
        this.blockId = -1;
        this.timeout = -1;
        this.ttl = -1;
        this.txnId = -1;
        this.threadId = -1;
        this.lockCount = 0;
        this.longValue = Long.MIN_VALUE;
        this.version = -1;
        this.indexes = null;
        this.indexTypes = null;
    }

    @Override
    public String toString() {
        return "Packet [callId = " + callId + "  name = " + name + " operation = " + operation + "]";
    }
}

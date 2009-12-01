/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.client.nio.Address;


public class Packet {	
	
	private int headerSize;
	
	private int keySize;
	
	private int valueSize;
	
	private byte[] headerInBytes;
	
	private byte[] key   ;
	
	private byte[] value ;
	
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
    
    private boolean lockAddressIsNull = true;

    private Address lockAddress;
    
	private long callId = -1;
	
	private boolean client = true;
	
	private byte indexCount = 0;

    private long[] indexes = new long[6];

    private byte[] indexTypes = new byte[6];

    public Packet() {
    }

    public void writeTo(DataOutputStream outputStream) throws IOException {
		headerInBytes = getHeader();
		headerSize = headerInBytes.length;
		outputStream.writeInt(headerSize);
		outputStream.writeInt(keySize);
		outputStream.writeInt(valueSize);
		outputStream.write(headerInBytes);
		if(key!=null)
			outputStream.write(key);
			
		if(value!=null)
			outputStream.write(value);
		
	}
	public void readFrom(DataInputStream dis) throws IOException {
		headerSize = dis.readInt();
		keySize = dis.readInt();
		valueSize = dis.readInt();
		headerInBytes = new byte[headerSize];
		dis.read(headerInBytes);
		
		
		ByteArrayInputStream bis = new ByteArrayInputStream(headerInBytes);
		DataInputStream dis2 = new DataInputStream(bis);
		this.operation = ClusterOperation.create(dis2.readInt());
		this.blockId = dis2.readInt();
		this.threadId = dis2.readInt();
		this.lockCount = dis2.readInt();
		this.timeout = dis2.readLong();
		this.ttl = dis2.readLong();
		this.txnId = dis2.readLong();
		this.longValue = dis2.readLong();
		this.version = dis2.readLong();
		this.callId = (int) dis2.readLong();
		this.client = dis2.readByte()==1;
		this.responseType = dis2.readByte();
		int nameLength = dis2.readInt();
		byte[] b = new byte[nameLength];
		dis2.read(b);
		this.name = new String(b);
		this.lockAddressIsNull = dis2.readBoolean();
        if (!lockAddressIsNull) {
            lockAddress = new Address();
            lockAddress.readData(dis2);
        }
	    indexCount = dis2.readByte();
        for (int i=0; i<indexCount ; i++) {
            indexes[i] = dis2.readLong();
            indexTypes[i] = dis2.readByte();
        }
		
		
		key = new byte[keySize];
		dis.read(key);
		value = new byte[valueSize];
		dis.read(value);
	}
	
	private byte[] getHeader() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeInt(operation.getValue());
		dos.writeInt(blockId);
		dos.writeInt(threadId);
		dos.writeInt(lockCount);
		dos.writeLong(timeout);
		dos.writeLong(ttl);
		dos.writeLong(txnId);
		dos.writeLong(longValue);
		dos.writeLong(version);
		dos.writeLong(callId);
		dos.writeByte(client?1:0);
		dos.writeByte(responseType);
		byte[] nameInBytes = name.getBytes();
		dos.writeInt(nameInBytes.length);
		dos.write(nameInBytes);
		dos.writeBoolean(lockAddressIsNull);
        if (!lockAddressIsNull) {
            lockAddress.writeData(dos);
        }
		dos.writeByte(indexCount);
        for (int i=0; i < indexCount; i++){
        	dos.writeLong(indexes[i]);
        	dos.writeByte(indexTypes[i]);
        }
		return bos.toByteArray();
	}
	public void set(String name, ClusterOperation operation,
			byte[] key, byte[] value) {
		this.name = name;
		this.operation  = operation;
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
		if(key!=null)
			keySize = this.key.length;
	}
	public byte[] getValue() {
		return value;
	}
	public void setValue(byte[] value) {
		this.value = value;
		if(value!=null)
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
	public boolean isLockAddressIsNull() {
		return lockAddressIsNull;
	}
	public void setLockAddressIsNull(boolean lockAddressIsNull) {
		this.lockAddressIsNull = lockAddressIsNull;
	}	
	public void setClient(boolean client) {
		this.client = client;
	}
	public boolean isClient() {
		return client;
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

    @Override
    public String toString(){
        return "Packet [callId = "+callId+"  name = "+ name +" operation = "+operation+"]";
    }
}

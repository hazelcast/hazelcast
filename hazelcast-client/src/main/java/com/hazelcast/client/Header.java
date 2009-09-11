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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.client.impl.ClusterOperation;
import com.hazelcast.client.nio.DataSerializable;

public class Header implements DataSerializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String name;
	private ClusterOperation operation;
	private int blockId = 0;
	private int threadId;
	private int lockCount = 0;
	private long timeout = -1;
	private long txnId;
	private long longValue;
	private long recordId = -1;
    public long version = -1;
    public byte responseType = Constants.ResponseTypes.RESPONSE_NONE;
    private boolean lockAddressIsNull = true;
	
	private int callId;
	
	
	
	
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
	public int getCallId() {
		return callId;
	}
	public void setCallId(int callId) {
		this.callId = callId;
	}
	public long getTxnId() {
		return txnId;
	}
	public void setTxnId(long txnId) {
		this.txnId = txnId;
	}
	public int getThreadId() {
		return threadId;
	}
	public void setThreadId(int threadId) {
		this.threadId = threadId;
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
	public void setBlockId(int blockId) {
		this.blockId = blockId;
	}
	public int getBlockId() {
		return blockId;
	}
	public void setLockCount(int lockCount) {
		this.lockCount = lockCount;
	}
	public int getLockCount() {
		return lockCount;
	}
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}
	public long getTimeout() {
		return timeout;
	}
	public void setRecordId(long recordId) {
		this.recordId = recordId;
	}
	public long getRecordId() {
		return recordId;
	}
	public void readData(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}
	public void writeData(DataOutput out) throws IOException {
		out.writeInt(operation.getValue());
		out.writeInt(blockId);
		out.writeInt(threadId);
		out.writeInt(lockCount);
		out.writeLong(timeout);
		out.writeLong(txnId);
		out.writeLong(longValue);
		out.writeLong(recordId);
		out.writeLong(version);
		out.writeLong(callId);
		out.writeByte(responseType);
		byte[] nameInBytes = name.getBytes();
		out.writeInt(nameInBytes.length);
		out.write(nameInBytes);
		out.writeBoolean(lockAddressIsNull);
	}
}

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

package com.hazelcast.cluster;

import java.io.IOException;

import com.hazelcast.map.operation.MapOperationType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class MemberAttributeChangedOperation extends AbstractClusterOperation {

	public static final byte DELTA_MEMBER_PROPERTIES_OP_PUT = 2;
	public static final byte DELTA_MEMBER_PROPERTIES_OP_REMOVE = 3;

	private MapOperationType operationType;
	private String key;
	private Object value;
	
	public MemberAttributeChangedOperation() {
	}
	
	public MemberAttributeChangedOperation(MapOperationType operationType, String key, Object value) {
		if (operationType != MapOperationType.PUT && operationType != MapOperationType.REMOVE) {
			throw new IllegalArgumentException("Only PUT / REMOVE operations are allowed for attribute updates");
		}
		this.operationType = operationType;
		this.key = key;
		this.value = value;
	}
	
	@Override
	public void run() throws Exception {
		final ClusterServiceImpl cs = getService();
		cs.updateMemberAttribute(getCallerUuid(), operationType, key, value);
	}

	@Override
	protected void writeInternal(ObjectDataOutput out) throws IOException {
		super.writeInternal(out);
		out.writeUTF(key);
		switch (operationType) {
			case PUT:
				out.writeByte(DELTA_MEMBER_PROPERTIES_OP_PUT);
				out.writeObject(value);
				break;
			case REMOVE:
				out.writeByte(DELTA_MEMBER_PROPERTIES_OP_REMOVE);
				break;
		}
	}

	@Override
	protected void readInternal(ObjectDataInput in) throws IOException {
		super.readInternal(in);
		key = in.readUTF();
		int operation = in.readByte();
		switch (operation)
		{
			case DELTA_MEMBER_PROPERTIES_OP_PUT:
				operationType = MapOperationType.PUT;
				value = in.readObject();
				break;
			case DELTA_MEMBER_PROPERTIES_OP_REMOVE:
				operationType = MapOperationType.REMOVE;
				break;
		}
	}

}

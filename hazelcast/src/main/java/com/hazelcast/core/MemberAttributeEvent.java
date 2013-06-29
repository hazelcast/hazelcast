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

package com.hazelcast.core;

import java.io.IOException;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.operation.MapOperationType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import static com.hazelcast.cluster.MemberAttributeChangedOperation.*;

public class MemberAttributeEvent extends MembershipEvent {

	private MapOperationType operationType;
	private String key; 
	private Object value;
	
	public MemberAttributeEvent() {
	}
	
	public MemberAttributeEvent(MemberImpl member, MapOperationType operationType, String key, Object value) {
		super(member, MEMBER_ATTRIBUTE_CHANGED);
		
	}

	public MapOperationType getOperationType() {
		return operationType;
	}

	public String getKey() {
		return key;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		super.writeData(out);
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
	public void readData(ObjectDataInput in) throws IOException {
		super.readData(in);
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

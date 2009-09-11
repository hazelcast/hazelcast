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

package com.hazelcast.client.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.hazelcast.client.Serializer;
import com.hazelcast.client.nio.DataSerializable;

public class Keys implements DataSerializable{
	/**
	 * 
	 */
	private Set<Object> keys = new HashSet<Object>();
	
	public Keys() {
	
	}
	public void readData(DataInput in) throws IOException {
		int size = in.readInt();
		keys = new HashSet<Object>();
		for(int i=0;i<size;i++){
			int length = in.readInt();
			byte[] data = new byte[length];
			in.readFully(data);
			Object obj = Serializer.toObject(data);
			keys.add(obj);
		}
	}
	
	public void writeData(DataOutput out) throws IOException {
//		int size = (keys==null)?0:keys.size();
//		out.writeInt(size);
//		if(size>0){
//			for (Object key : keys) {
//				System.out.println("KEY: "+key);
//				out.write(((byte[])key).length);
//				out.write((byte[])key);
//			}
//		}
		throw new UnsupportedOperationException();
	}
	
	public Set getKeys(){
		return keys;
	}
	public void setKeys(Set keys){
		this.keys = keys;
	}
	public void addKey(byte[] obj) {
		this.keys.add(obj);
		
	}
	
}
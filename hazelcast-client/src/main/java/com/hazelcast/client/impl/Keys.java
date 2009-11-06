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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.client.Serializer.toObject;
import com.hazelcast.nio.DataSerializable;

public class Keys<K> implements DataSerializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3983785771408545165L;
	/**
	 * 
	 */
	private Collection<K> keys;
	
	public Keys() {
	
	}
	public void readData(DataInput in) throws IOException {
		int size = in.readInt();
		keys = new ArrayList<K>();
		for(int i=0;i<size;i++){
			int length = in.readInt();
			byte[] data = new byte[length];
			in.readFully(data);
			K obj = (K)toObject(data);
			System.out.println();
			keys.add(obj);
		}
	}
	
	public void writeData(DataOutput out) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	public Collection<K> getKeys(){
		return keys;
	}
	public void setKeys(Set<K> keys){
		this.keys = keys;
	}
	public void addKey(byte[] obj) {
		this.keys.add((K)toObject(obj));
		
	}
	
}
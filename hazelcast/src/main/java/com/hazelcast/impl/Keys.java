package com.hazelcast.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

public class Keys implements DataSerializable{
	/**
	 * 
	 */
	private Set<Data> keys = new HashSet<Data>();
	
	public Keys() {
	
	}
	public void readData(DataInput in) throws IOException {
		int size = in.readInt();
		keys = new HashSet<Data>();
		for(int i=0;i<size;i++){
			Data data = new Data();
			data.readData(in);
			keys.add(data);
		}
	}
	
	public void writeData(DataOutput out) throws IOException {
		int size = (keys==null)?0:keys.size();
		out.writeInt(size);
		if(size>0){
			for (Data key : keys) {
				System.out.println("KEY: "+key);
//				Data d = ThreadContext.get().toData(key);
				key.writeData(out);
//				ThreadContext.get().hardCopy(d).writeData(out);
			}
		}
	}
	
	public Set getKeys(){
		return keys;
	}
	public void setKeys(Set keys){
		this.keys = keys;
	}
	public void addKey(Data obj) {
		this.keys.add(obj);
		
	}
	
}
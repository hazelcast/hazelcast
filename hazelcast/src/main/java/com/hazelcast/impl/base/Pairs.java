/**
 * 
 */
package com.hazelcast.impl.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.nio.DataSerializable;

public class Pairs implements DataSerializable {
    private List<KeyValue> lsKeyValues = null;

    public Pairs() {
    }

    public void addKeyValue(KeyValue keyValue) {
        if (getLsKeyValues() == null) {
            setLsKeyValues(new ArrayList<KeyValue>());
        }
        getLsKeyValues().add(keyValue);
    }

    public void writeData(DataOutput out) throws IOException {
        int size = (getLsKeyValues() == null) ? 0 : getLsKeyValues().size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            getLsKeyValues().get(i).writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            if (getLsKeyValues() == null) {
                setLsKeyValues(new ArrayList<KeyValue>());
            }
            KeyValue kv = new KeyValue();
            kv.readData(in);
            getLsKeyValues().add(kv);
        }
    }

    public int size() {
        return (getLsKeyValues() == null) ? 0 : getLsKeyValues().size();
    }

    public KeyValue getEntry(int i) {
        return (getLsKeyValues() == null) ? null : getLsKeyValues().get(i);
    }

	/**
	 * @param lsKeyValues the lsKeyValues to set
	 */
	public void setLsKeyValues(List<KeyValue> lsKeyValues) {
		this.lsKeyValues = lsKeyValues;
	}

	/**
	 * @return the lsKeyValues
	 */
	public List<KeyValue> getLsKeyValues() {
		return lsKeyValues;
	}
}
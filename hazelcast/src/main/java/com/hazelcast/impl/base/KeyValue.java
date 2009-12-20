/**
 * 
 */
package com.hazelcast.impl.base;

import static com.hazelcast.nio.IOUtil.toObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.FactoryImpl.IGetAwareProxy;
import com.hazelcast.impl.FactoryImpl.MProxy;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

public class KeyValue implements Map.Entry, DataSerializable {
    Data key = null;
    Data value = null;
    Object objKey = null;
    Object objValue = null;
    String name = null;
    FactoryImpl factory;

    public KeyValue() {
    }

    public KeyValue(Data key, Data value) {
        this.key = key;
        this.value = value;
    }

    public void writeData(DataOutput out) throws IOException {
        key.writeData(out);
        boolean gotValue = (value != null && value.size() > 0);
        out.writeBoolean(gotValue);
        if (gotValue) {
            value.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        key = new Data();
        key.readData(in);
        boolean gotValue = in.readBoolean();
        if (gotValue) {
            value = new Data();
            value.readData(in);
        }
    }

    public Data getKeyData() {
        return key;
    }

    public Data getValueData() {
        return value;
    }

    public Object getKey() {
        if (objKey == null) {
            objKey = toObject(key);
        }
        return objKey;
    }

    public Object getValue() {
        if (objValue == null) {
            if (value != null) {
                objValue = toObject(value);
            } else {
                objValue = ((FactoryImpl.IGetAwareProxy) factory.getOrCreateProxyByName(name)).get((key == null) ? getKey() : key);
            }
        }
        return objValue;
    }

    public Object setValue(Object newValue) {
        if (name == null) throw new UnsupportedOperationException();
        this.objValue = value;
        return ((FactoryImpl.MProxy) factory.getOrCreateProxyByName(name)).put(key, newValue);
    }

    public void setName(FactoryImpl factoryImpl, String name) {
        this.factory = factoryImpl;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Map.Entry key=" + getKey() + ", value=" + getValue();
    }
}
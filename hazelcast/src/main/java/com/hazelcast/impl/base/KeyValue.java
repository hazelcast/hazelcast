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

package com.hazelcast.impl.base;

import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.IGetAwareProxy;
import com.hazelcast.impl.MProxy;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.nio.IOUtil.toObject;

public class KeyValue implements Map.Entry, DataSerializable {
    protected Data key = null;
    protected Data value = null;
    protected Object objKey = null;
    protected Object objValue = null;
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
                objValue = ((IGetAwareProxy) factory.getOrCreateProxyByName(name)).get((key == null) ? getKey() : key);
            }
        }
        return objValue;
    }

    public Object setValue(Object newValue) {
        if (name == null) throw new UnsupportedOperationException();
        this.objValue = value;
        return ((MProxy) factory.getOrCreateProxyByName(name)).put(key, newValue);
    }

    public void setName(FactoryImpl factoryImpl, String name) {
        this.factory = factoryImpl;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValue keyValue = (KeyValue) o;
        if (key != null ? !key.equals(keyValue.key) : keyValue.key != null) return false;
        if (name != null ? !name.equals(keyValue.name) : keyValue.name != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Map.Entry key=" + getKey() + ", value=" + getValue();
    }
}

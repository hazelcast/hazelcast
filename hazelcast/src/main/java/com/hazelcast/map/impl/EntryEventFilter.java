/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.EventFilter;

import java.io.IOException;

public class EntryEventFilter implements EventFilter, IdentifiedDataSerializable {

    protected boolean includeValue;
    protected Data key;

    public EntryEventFilter(boolean includeValue, Data key) {
        this.includeValue = includeValue;
        this.key = key;
    }

    public EntryEventFilter() {
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public Data getKey() {
        return key;
    }

    @Override
    public boolean eval(Object arg) {
        return key == null || key.equals(arg);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(includeValue);
        out.writeData(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        includeValue = in.readBoolean();
        key = in.readData();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EntryEventFilter that = (EntryEventFilter) o;

        if (includeValue != that.includeValue) {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = (includeValue ? 1 : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EntryEventFilter{"
                + "includeValue=" + includeValue
                + ", key=" + key
                + '}';
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.ENTRY_EVENT_FILTER;
    }
}

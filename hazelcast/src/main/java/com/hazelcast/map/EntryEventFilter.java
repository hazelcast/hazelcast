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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.EventFilter;

import java.io.IOException;

public class EntryEventFilter implements EventFilter,DataSerializable {

    boolean includeValue = false;
    Object key = null;

    public EntryEventFilter(boolean includeValue, Object key) {
        this.includeValue = includeValue;
        this.key = key;
    }

    public EntryEventFilter() {
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public Object getKey() {
        return key;
    }

    public boolean eval(Object arg) {
        return key == null || key.equals(arg);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(includeValue);
        out.writeObject(key);
    }

    public void readData(ObjectDataInput in) throws IOException {
        includeValue = in.readBoolean();
        key = in.readObject();
    }
}

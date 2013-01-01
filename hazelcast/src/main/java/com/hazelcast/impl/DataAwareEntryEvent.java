/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Member;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.serialization.SerializerRegistry;

import static com.hazelcast.nio.IOUtil.toObject;

public class DataAwareEntryEvent extends EntryEvent {

    protected final Data dataKey;

    protected final Data dataNewValue;

    protected final Data dataOldValue;

    protected final boolean firedLocally;

    private final transient SerializerRegistry serializerRegistry;

    public DataAwareEntryEvent(Member from, int eventType,
                               String name, Data dataKey,
                               Data dataNewValue, Data dataOldValue,
                               boolean firedLocally,
                               SerializerRegistry serializerRegistry) {
        super(name, from, eventType, null, null);
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
        this.firedLocally = firedLocally;
        this.serializerRegistry = serializerRegistry;
    }

    public Data getKeyData() {
        return dataKey;
    }

    public Data getNewValueData() {
        return dataNewValue;
    }

    public Data getOldValueData() {
        return dataOldValue;
    }

    public Object getKey() {
        if (key == null && dataKey != null) {
            beforeReadData();
            key = toObject(dataKey);
        }
        return key;
    }

    public Object getOldValue() {
        if (oldValue == null && dataOldValue != null) {
            beforeReadData();
            oldValue = toObject(dataOldValue);
        }
        return oldValue;
    }

    public Object getValue() {
        if (value == null && dataNewValue != null) {
            beforeReadData();
            value = toObject(dataNewValue);
        }
        return value;
    }

    private void beforeReadData() {
        if (serializerRegistry != null) {
            ThreadContext.get().setCurrentSerializerRegistry(serializerRegistry);
        }
    }

    public String getLongName() {
        return name;
    }
}

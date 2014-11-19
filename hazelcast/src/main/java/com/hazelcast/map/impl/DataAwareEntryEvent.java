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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Member;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;

public class DataAwareEntryEvent extends EntryEvent {

    private static final long serialVersionUID = 1;

    private final transient Data dataKey;

    private final transient Data dataNewValue;

    private final transient Data dataOldValue;

    private final transient Data dataMergingValue;

    private final transient SerializationService serializationService;

    public DataAwareEntryEvent(Member from, int eventType,
                               String source, Data dataKey,
                               Data dataNewValue, Data dataOldValue,
                               Data dataMergingValue,
                               SerializationService serializationService) {
        super(source, from, eventType, null, null);
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
        this.dataMergingValue = dataMergingValue;
        this.serializationService = serializationService;
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

    public Data getMergingValueData() {
        return dataMergingValue;
    }

    public Object getKey() {
        if (key == null && dataKey != null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
    }

    public Object getOldValue() {
        if (oldValue == null && dataOldValue != null) {
            oldValue = serializationService.toObject(dataOldValue);
        }
        return oldValue;
    }

    public Object getValue() {
        if (value == null && dataNewValue != null) {
            value = serializationService.toObject(dataNewValue);
        }
        return value;
    }

    public Object getMergingValue() {
        if (mergingValue == null && dataMergingValue != null) {
            mergingValue = serializationService.toObject(dataMergingValue);
        }
        return mergingValue;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        throw new NotSerializableException();
    }
}

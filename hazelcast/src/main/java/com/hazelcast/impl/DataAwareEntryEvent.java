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

package com.hazelcast.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Data;

import static com.hazelcast.nio.IOUtil.toObject;

public class DataAwareEntryEvent extends EntryEvent {
    protected final Data dataKey;

    protected final Data dataNewValue;

    protected final Data dataOldValue;

    protected final boolean firedLocally;

    public DataAwareEntryEvent(Member from, int eventType,
                               String name, Data dataKey,
                               Data dataNewValue, Data dataOldValue, boolean firedLocally) {
        super(name, from, eventType, null, null);
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
        this.firedLocally = firedLocally;
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
            key = toObject(dataKey);
        }
        return key;
    }

    public Object getOldValue() {
        if (oldValue == null && dataOldValue != null) {
            oldValue = toObject(dataOldValue);
        }
        return oldValue;
    }

    public Object getValue() {
        if (value == null && dataNewValue != null) {
            value = toObject(dataNewValue);
        }
        return value;
    }

    public String getLongName() {
        return name;
    }
}

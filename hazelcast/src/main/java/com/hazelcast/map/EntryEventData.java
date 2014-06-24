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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import java.io.IOException;

/**
 * An entry's event data.
 */
public class EntryEventData extends AbstractEventData {

    protected Data dataKey;
    protected Data dataNewValue;
    protected Data dataOldValue;

    public EntryEventData() {
    }

    public EntryEventData(String source, String mapName, Address caller,
                          Data dataKey, Data dataNewValue, Data dataOldValue, int eventType) {
        super(source, mapName, caller, eventType);
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
    }

    public Data getDataKey() {
        return dataKey;
    }

    public Data getDataNewValue() {
        return dataNewValue;
    }

    public Data getDataOldValue() {
        return dataOldValue;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        dataKey.writeData(out);
        IOUtil.writeNullableData(out, dataNewValue);
        IOUtil.writeNullableData(out, dataOldValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        dataKey = IOUtil.readData(in);
        dataNewValue = IOUtil.readNullableData(in);
        dataOldValue = IOUtil.readNullableData(in);
    }

    public Object cloneWithoutValues() {
        return new EntryEventData(getSource(), getMapName(), getCaller(), dataKey, null, null, getEventType());
    }

    @Override
    public String toString() {
        return "EntryEventData{"
                + super.toString()
                + '}';
    }
}
